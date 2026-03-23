/* ── Bookmarks (browser-links) ───────────────────────────────────────── */

// ── Known-field renderers and metadata ───────────────────────────────────
// Only fields with special rendering logic need an entry here.
// Any field that arrives from the API and is NOT listed gets a plain-text
// fallback renderer automatically — that is what makes this data-driven.
const _BM_FIELD_META = {
  _icon:       { label: 'Icon',        sortKey: null,
                 render: b => `<td style="text-align:center;width:30px">${b._item_type === 'visit' ? '&#128065;' : '&#128278;'}</td>` },
  title:       { label: 'Title',       sortKey: 'title',
                 render: b => `<td><a href="${esc(b.url)}" target="_blank" rel="noopener noreferrer" style="color:var(--accent)">${esc(b.title || b.url)}</a></td>` },
  url:         { label: 'URL',         sortKey: 'url',
                 render: b => `<td style="font-size:11px;color:var(--text-dim);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(b.url)}">${_bmTruncUrl(b.url)}</td>` },
  tags:        { label: 'Tags',        sortKey: 'tags',
                 render: b => `<td style="font-size:11px">${(b.tags||[]).map(t=>_bmTagPill(t)).join(' ')}</td>` },
  description: { label: 'Description', sortKey: 'description',
                 render: b => `<td style="font-size:12px;color:var(--text-dim);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(b.description||b.notes||'')}</td>` },
  notes:       { label: 'Notes',       sortKey: null,
                 render: b => `<td style="font-size:12px;color:var(--text-dim);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(b.notes||'')}</td>` },
  source:      { label: 'Source',      sortKey: 'source',
                 render: b => `<td style="font-size:11px;color:var(--text-dim)">${esc(b.source||'')}</td>` },
  created_at:  { label: 'Saved',       sortKey: 'created_at',
                 render: b => `<td style="font-size:11px;color:var(--text-dim);white-space:nowrap">${_bmFmtDate(b.created_at||b.visited_at||'')}</td>` },
  updated_at:  { label: 'Updated',     sortKey: 'updated_at',
                 render: b => `<td style="font-size:11px;color:var(--text-dim);white-space:nowrap">${_bmFmtDate(b.updated_at||'')}</td>` },
  folder:      { label: 'Folder',      sortKey: null,
                 render: b => `<td style="font-size:11px;color:var(--text-dim)">${esc(b.folder||'')}</td>` },
  favicon_url: { label: 'Favicon',     sortKey: null,
                 render: b => b.favicon_url ? `<td><img src="${esc(b.favicon_url)}" style="width:16px;height:16px;vertical-align:middle" loading="lazy" /></td>` : '<td></td>' },
  archived:    { label: 'Archived',    sortKey: null,
                 render: b => `<td style="font-size:11px;color:var(--text-dim)">${b.archived ? 'Yes' : ''}</td>` },
  // ── Search transparency fields (hidden by default; appear only in search results) ──
  score_sources: { label: 'Sources',      sortKey: null,
                   render: b => {
                     const srcs = b.score_sources || [];
                     const pills = {
                       bookmark_keyword: ['KW-BM', '#d97706'],
                       visit_keyword:    ['KW-V',  '#b45309'],
                       bookmark_vector:  ['VEC-BM','#2563eb'],
                       visit_vector:     ['VEC-V', '#7c3aed'],
                     };
                     const html = srcs.map(s => {
                       const [label, color] = pills[s] || [s, '#6b7280'];
                       return `<span style="font-size:10px;padding:1px 5px;border-radius:9px;background:${color};color:#fff;white-space:nowrap">${label}</span>`;
                     }).join(' ');
                     return `<td><span class="bm-score-cell" data-metric="score_sources" style="cursor:pointer" title="Click to analyse score">${html}</span></td>`;
                   } },
  rrf_score:     { label: 'RRF Score',    sortKey: 'rrf_score',
                   render: b => {
                     const v = b.rrf_score;
                     return `<td><span class="bm-score-cell" data-metric="rrf_score" style="cursor:pointer;font-size:11px;font-variant-numeric:tabular-nums;color:var(--text-dim)" title="Click to analyse score">${v != null ? v.toFixed(5) : ''}</span></td>`;
                   } },
  kw_tier:       { label: 'KW Tier',      sortKey: 'kw_tier',
                   render: b => {
                     const tier = b.kw_tier;
                     if (tier == null) return '<td></td>';
                     const labels = ['Phrase in title','Phrase in URL','All tokens cross-field','Phrase in tags','Token in title','Token in URL','Token in tags','Document only'];
                     const colors = ['#059669','#059669','#16a34a','#ca8a04','#ca8a04','#ea580c','#ea580c','#6b7280'];
                     return `<td><span class="bm-score-cell" data-metric="kw_tier" style="cursor:pointer;font-size:11px;white-space:nowrap;color:${colors[tier]||'#6b7280'}" title="${labels[tier]||''} — Click to analyse">${tier} – ${labels[tier]||'?'}</span></td>`;
                   } },
  cosine_distance:{ label: 'Cos Dist',    sortKey: 'cosine_distance',
                   render: b => {
                     const v = b.cosine_distance;
                     if (v == null) return '<td></td>';
                     const color = v < 0.3 ? '#059669' : v < 0.6 ? '#ca8a04' : '#6b7280';
                     return `<td><span class="bm-score-cell" data-metric="cosine_distance" style="cursor:pointer;font-size:11px;font-variant-numeric:tabular-nums;color:${color}" title="Click to analyse score">${v.toFixed(4)}</span></td>`;
                   } },
  reranker_rank: { label: 'Reranker',     sortKey: 'reranker_rank',
                   render: b => {
                     const v = b.reranker_rank;
                     return `<td><span class="bm-score-cell" data-metric="reranker_rank" style="cursor:pointer;font-size:11px;color:var(--text-dim)" title="Click to analyse score">${v != null ? '#' + v : ''}</span></td>`;
                   } },
  exact_tier:    { label: 'Exact Tier',   sortKey: 'exact_tier',
                   render: b => {
                     const tier = b.exact_tier;
                     if (tier == null) return '<td></td>';
                     const labels = ['Phrase in title/URL','All tokens cross-field','Token in title/URL','Tags only','Pure embedding'];
                     const colors = ['#059669','#16a34a','#ca8a04','#ea580c','#6b7280'];
                     return `<td><span class="bm-score-cell" data-metric="exact_tier" style="cursor:pointer;font-size:11px;white-space:nowrap;color:${colors[tier]||'#6b7280'}" title="${labels[tier]||''} — Click to analyse">${tier} – ${labels[tier]||'?'}</span></td>`;
                   } },
  _actions:    { label: 'Actions',     sortKey: null,
                 render: b => {
                   if (b._item_type === 'visit') return '<td></td>';
                   const archBtn = b.archived
                     ? `<button class="secondary" style="padding:1px 6px;font-size:11px;color:var(--ok);border-color:var(--ok);margin-left:2px" title="Restore from archive" onclick="archiveBookmark('${esc(b.bookmark_id)}', true)">&#128228;</button>`
                     : `<button class="secondary" style="padding:1px 6px;font-size:11px;color:var(--text-dim);border-color:var(--border);margin-left:2px" title="Archive" onclick="archiveBookmark('${esc(b.bookmark_id)}', false)">&#128229;</button>`;
                   return `<td style="white-space:nowrap;width:110px">
                     <button class="secondary" style="padding:1px 6px;font-size:11px" onclick="openBookmarkModal('${esc(b.bookmark_id)}')">&#9998;</button>${archBtn}
                     <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:2px" onclick="deleteBookmark('${esc(b.bookmark_id)}','${esc(b.title||b.url)}')">&#x2715;</button></td>`;
                 } },
};

// Fields never shown as columns (used internally for operations)
const _BM_EXCLUDE_COLS = new Set(['bookmark_id', '_item_type']);

// Fields hidden by default on first visit (user can show via Columns modal)
const _BM_DEFAULT_HIDDEN = ['notes', 'folder', 'favicon_url', 'updated_at', 'archived',
  'score_sources', 'rrf_score', 'kw_tier', 'cosine_distance', 'reranker_rank', 'exact_tier'];

// Convert a snake_case key to a human label (fallback for unknown fields)
function _bmFieldLabel(key) {
  const m = _BM_FIELD_META[key];
  if (m) return m.label;
  return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

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
let _bmLastSearchResults = []; // cached for re-render on column toggle

// ── Pagination state ─────────────────────────────────────────────────────
// Visual-only pagination: full _bookmarks array is always loaded; only the
// rendered slice changes.  All client-side filter/sort/search still operates
// on the complete dataset — only the table render is sliced.
const _BM_PAGE_SIZE_OPTIONS = [25, 50, 100, 250, 1000, 2000, 5000, 10000];
let _bmPage = 1;
let _bmPageSize = (() => {
  const n = parseInt(localStorage.getItem('bm-page-size') || '100', 10);
  return _BM_PAGE_SIZE_OPTIONS.includes(n) ? n : 100;
})();

// Dynamic column list — derived from actual API response keys, not hardcoded
let _bmDynCols = []; // populated by _bmDetectCols(); drives everything

function _bmSetSearchActive(active) {
  _bmSearchActive = active;
  const btn = document.getElementById('bm-explain-sort-btn');
  if (btn) btn.disabled = !active;
}

// Hidden cols: persisted to localStorage. On first visit, apply default hidden set.
const _bmHiddenColsRaw = localStorage.getItem('bm-hidden-cols');
let _bmHiddenCols = new Set(_bmHiddenColsRaw ? JSON.parse(_bmHiddenColsRaw) : _BM_DEFAULT_HIDDEN);

// Called after each API load — derives column list from actual response keys.
// Preserves existing order for known cols; appends any new/unknown cols at end.
// _icon and _actions are synthetic (not from API): always first and last.
function _bmDetectCols(rows) {
  const apiKeys = rows.length ? Object.keys(rows[0]).filter(k => !_BM_EXCLUDE_COLS.has(k)) : [];
  const existingApiCols = _bmDynCols.filter(k => k !== '_icon' && k !== '_actions');
  const existingSet = new Set(existingApiCols);
  const newSet = new Set(apiKeys);
  _bmDynCols = [
    '_icon',
    ...existingApiCols.filter(k => newSet.has(k)), // keep order, drop removed
    ...apiKeys.filter(k => !existingSet.has(k)),   // append new keys
    '_actions',
  ];
}

function _bmVisibleDataCols() {
  return _bmDynCols.filter(k => !_bmHiddenCols.has(k));
}

function _bmColCount() { return _bmVisibleDataCols().length; }

function _bmRebuildThead() {
  const tr = document.querySelector('#bm-main-view thead tr');
  if (!tr) return;
  let html = '';
  for (const key of _bmVisibleDataCols()) {
    const sortKey = _BM_FIELD_META[key]?.sortKey ?? null;
    const label   = _bmFieldLabel(key);
    const style   = key === '_icon' ? ' style="width:30px"' : key === '_actions' ? ' style="width:110px"' : '';
    html += sortKey
      ? `<th class="bm-th-sort" onclick="_bmSortBy('${sortKey}')"${style}>${label}<span class="bm-sort-arrow" data-col="${sortKey}">&#x21C5;</span></th>`
      : `<th${style}>${label}</th>`;
  }
  tr.innerHTML = html;
  _bmColResizeDone = false;
  _bmUpdateSortHeaders();
}

function _bmRenderDataTds(b) {
  return _bmVisibleDataCols().map(key => {
    const meta = _BM_FIELD_META[key];
    if (meta) return meta.render(b);
    // Unknown field — plain text fallback; this is the data-driven path
    const val = b[key];
    const text = val == null ? '' : (Array.isArray(val) ? val.join(', ') : String(val));
    return `<td style="font-size:11px;color:var(--text-dim)">${esc(text)}</td>`;
  }).join('');
}

function _bmBuildBookmarkRow(b) {
  const archiveStyle = b.archived ? 'opacity:0.55' : '';
  const row = { ...b, _item_type: 'bookmark' };
  return `<tr style="${archiveStyle}">${_bmRenderDataTds(row)}</tr>`;
}

function _bmBuildSearchRow(r, scoreIdx) {
  const isBookmark = r.item_type !== 'visit';
  const b = { ...r, bookmark_id: r.id, _item_type: r.item_type || 'bookmark' };
  if (!isBookmark && r.visited_at && !b.created_at) b.created_at = r.visited_at;
  const idxAttr = scoreIdx != null ? ` data-score-idx="${scoreIdx}"` : '';
  return `<tr${idxAttr}>${_bmRenderDataTds(b)}</tr>`;
}

// ── Column visibility modal ──────────────────────────────────────────────
// Modal list is built from _bmDynCols — whatever the API actually returned.
// No column names are hardcoded here.
function _bmOpenColsModal() {
  const list = document.getElementById('bm-cols-modal-list');
  list.innerHTML = _bmDynCols.map(key => {
    const label   = _bmFieldLabel(key);
    const checked = !_bmHiddenCols.has(key) ? 'checked' : '';
    return `<label style="display:flex;align-items:center;gap:10px;cursor:pointer;padding:8px 4px;font-size:13px;border-bottom:1px solid var(--border)">
      <input type="checkbox" data-col="${key}" ${checked} style="width:15px;height:15px;cursor:pointer" />
      <span>${label}</span>
    </label>`;
  }).join('');
  document.getElementById('bm-cols-modal').showModal();
}

function _bmApplyColsModal() {
  const modal = document.getElementById('bm-cols-modal');
  // Start from existing hidden set — only update columns that were actually
  // shown in this modal.  Columns not in the modal (e.g. search-only fields
  // like domain/item_type/score cols when the modal was opened in browse mode)
  // keep their current hidden/visible state and are NOT implicitly un-hidden.
  const newHidden = new Set(_bmHiddenCols);
  modal.querySelectorAll('input[data-col]').forEach(cb => {
    if (cb.checked) {
      newHidden.delete(cb.dataset.col); // user made it visible
    } else {
      newHidden.add(cb.dataset.col);    // user hid it
    }
  });
  _bmHiddenCols = newHidden;
  localStorage.setItem('bm-hidden-cols', JSON.stringify([..._bmHiddenCols]));
  _bmRebuildThead();
  if (_bmSearchActive) {
    _renderBmSearchResults(_bmLastSearchResults);
  } else {
    renderBookmarks({ keepPage: true }); // column toggle — stay on current page
  }
  modal.close();
}

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
    const limit = parseInt(getFrontendSetting('bm_fetch_limit', 50000), 10);
    const r = await apiFetch(`/api/v1/bookmarks?archived=${archived}&limit=${limit}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _bookmarks = await r.json();
    _bmDetectCols(_bookmarks);  // derive column list from actual API response keys
    _bmSetSearchActive(false);
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
    _bmSetSearchActive(false);
    document.getElementById('bm-search-status').hidden = true;
    renderBookmarks();
    return;
  }
  // Debounce client-side filter (250ms) — fast enough to feel responsive,
  // slow enough to avoid rebuilding the full table on every keystroke.
  _bmRenderTimer = setTimeout(() => {
    _bmSetSearchActive(false);
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
      _bmSetSearchActive(true);
      status.textContent = `SeekDB: ${data.count} result${data.count === 1 ? '' : 's'} for "${q}"`;
      status.hidden = false;
      _renderBmSearchResults(data.results);
    } else {
      // SeekDB has no results (likely not yet indexed) — keep client-side filter
      _bmSetSearchActive(false);
      status.textContent = `SeekDB: 0 results for "${q}"`;
      status.hidden = false;
    }
  } catch (e) {
    // SeekDB unavailable — client-side filter already showing, suppress error
    _bmSetSearchActive(false);
  }
}

function _renderBmSearchResults(results) {
  _bmLastSearchResults = results;
  // Search results replace the paginated browse view — clear pagination controls.
  const pag = document.getElementById('bm-pagination');
  if (pag) pag.innerHTML = '';
  const tagFilter = document.getElementById('bm-tag-filter')?.value || '';
  let rows = tagFilter ? results.filter(r => (r.tags || []).includes(tagFilter)) : results;
  const tbody = document.getElementById('bm-tbody');
  const status = document.getElementById('bm-search-status');
  if (!rows.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="${_bmColCount()}">No results found.</td></tr>`;
    if (status) { status.textContent = '0 results'; status.hidden = false; }
    return;
  }
  _bmDetectCols(rows);  // search results have extra fields (score_sources, rrf_score, etc.)
  _bmRebuildThead();
  tbody.innerHTML = rows.map((r, i) => _bmBuildSearchRow(r, i)).join('');
  if (status) { status.textContent = rows.length + ' result' + (rows.length === 1 ? '' : 's') + (tagFilter ? ` (tag: ${tagFilter})` : ''); status.hidden = false; }
}

// ── Render table (local filter, no SeekDB) ──────────────────────────────
// opts.keepPage — if true, stay on the current page instead of resetting to 1.
// Pass keepPage=true when only the column visibility or a single row changed;
// leave it false (default) for filter/sort/load changes so the user always
// sees results from the start.

function renderBookmarks(opts = {}) {
  if (_bmSearchActive) return;
  if (!opts.keepPage) _bmPage = 1;
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
  // Pagination: slice the filtered+sorted array for rendering.
  // The full rows array is always processed above — only the table render is limited.
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / _bmPageSize));
  _bmPage = Math.max(1, Math.min(_bmPage, totalPages)); // clamp to valid range
  const pageRows = totalRows > _bmPageSize
    ? rows.slice((_bmPage - 1) * _bmPageSize, _bmPage * _bmPageSize)
    : rows;
  const tbody = document.getElementById('bm-tbody');
  const status = document.getElementById('bm-search-status');
  if (pageRows.length < totalRows) {
    const from = (_bmPage - 1) * _bmPageSize + 1;
    const to   = Math.min(_bmPage * _bmPageSize, totalRows);
    status.textContent = `${totalRows} bookmark${totalRows === 1 ? '' : 's'} (showing ${from}\u2013${to})`;
  } else {
    status.textContent = totalRows + ' bookmark' + (totalRows === 1 ? '' : 's');
  }
  status.hidden = false;
  _bmRebuildThead();
  if (!pageRows.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="${_bmColCount()}">No bookmarks found.</td></tr>`;
    _bmUpdateSortHeaders();
    _bmRenderPagination(totalRows, _bmPageSize, _bmPage);
    return;
  }
  tbody.innerHTML = pageRows.map(b => _bmBuildBookmarkRow(b)).join('');
  _bmUpdateSortHeaders();
  _bmInitColResize();
  _bmRenderPagination(totalRows, _bmPageSize, _bmPage);
}

// ── Pagination controls ──────────────────────────────────────────────────

function _bmRenderPagination(total, pageSize, page) {
  const el = document.getElementById('bm-pagination');
  if (!el) return;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  if (totalPages <= 1) { el.innerHTML = ''; return; }
  const sizeOpts = _BM_PAGE_SIZE_OPTIONS.map(n =>
    `<option value="${n}"${n === pageSize ? ' selected' : ''}>${n}</option>`
  ).join('');
  const prevDis = page <= 1 ? ' disabled' : '';
  const nextDis = page >= totalPages ? ' disabled' : '';
  el.innerHTML = `
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:12px;color:var(--text-dim);padding:8px 0 4px">
      <button class="secondary" style="padding:2px 10px;font-size:12px"${prevDis} onclick="_bmGoPage(${page - 1})">&#8592; Prev</button>
      <span>Page <strong style="color:var(--text)">${page}</strong> of <strong style="color:var(--text)">${totalPages}</strong></span>
      <button class="secondary" style="padding:2px 10px;font-size:12px"${nextDis} onclick="_bmGoPage(${page + 1})">Next &#8594;</button>
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;margin-left:8px">
        Per page:
        <select onchange="_bmSetPageSize(parseInt(this.value,10))" style="font-size:12px;padding:2px 6px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text)">${sizeOpts}</select>
      </label>
    </div>`;
}

function _bmGoPage(n) {
  _bmPage = n; // renderBookmarks will clamp to valid range via Math.min
  renderBookmarks({ keepPage: true });
}

function _bmSetPageSize(n) {
  if (!_BM_PAGE_SIZE_OPTIONS.includes(n)) return;
  _bmPageSize = n;
  localStorage.setItem('bm-page-size', String(n));
  _bmPage = 1;
  renderBookmarks({ keepPage: true });
}

// ── Visits ──────────────────────────────────────────────────────────────

// ── Visit column metadata ────────────────────────────────────────────────
const _VIS_FIELD_META = {
  title:       { label: 'Title',       render: v => `<td>${esc(v.title || '')}</td>` },
  url:         { label: 'URL',         render: v => `<td style="font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(v.url)}"><a href="${esc(v.url)}" target="_blank" rel="noopener noreferrer" style="color:var(--text-dim);text-decoration:none" onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">${_bmTruncUrl(v.url)}</a></td>` },
  domain:      { label: 'Domain',      render: v => `<td style="font-size:11px;color:var(--text-dim)">${esc(v.domain || '')}</td>` },
  source:      { label: 'Source',      render: v => `<td style="font-size:11px;color:var(--text-dim)">${esc(v.source || '')}</td>` },
  dwell_seconds:{ label: 'Dwell',      render: v => `<td style="font-size:11px;color:var(--text-dim)">${v.dwell_seconds ? v.dwell_seconds + 's' : '—'}</td>` },
  visit_count: { label: 'Times',       render: v => `<td style="font-size:11px;text-align:center">${v.visit_count > 1 ? `<span style="font-weight:600;color:var(--accent)">${v.visit_count}</span>` : `<span style="color:var(--text-dim)">1</span>`}</td>` },
  visited_at:  { label: 'Visited',     render: v => `<td style="font-size:11px;color:var(--text-dim);white-space:nowrap">${_bmFmtDate(v.visited_at || '')}</td>` },
  _actions:    { label: 'Actions',     render: v => {
    const expandId = `ve-${esc(v.visit_id)}`;
    const saveBtn = v.bookmark_id ? '' :
      `<button class="secondary" style="padding:1px 6px;font-size:11px" onclick="promoteVisitToBookmark('${esc(v.url)}','${esc(v.title || '')}')">&#128278; Save</button>`;
    const expandBtn = v.visit_count > 1
      ? `<button class="secondary" style="padding:1px 6px;font-size:11px" title="Show individual visit times" onclick="_bmToggleVisitEvents('${esc(v.normalized_url)}','${expandId}')">&#128337;</button>`
      : '';
    return `<td style="white-space:nowrap">${saveBtn} ${expandBtn}</td>`;
  }},
};

const _VIS_ALL_COLS    = ['title', 'url', 'domain', 'source', 'dwell_seconds', 'visit_count', 'visited_at', '_actions'];
const _VIS_DEFAULT_HIDDEN = ['domain'];
// sortKey: the field name to sort on (null = not sortable)
const _VIS_SORT_KEYS = { title: 'title', url: 'url', domain: 'domain', source: 'source',
  dwell_seconds: 'dwell_seconds', visit_count: 'visit_count', visited_at: 'visited_at' };

const _visHiddenColsRaw = localStorage.getItem('vis-hidden-cols');
let _visHiddenCols = new Set(_visHiddenColsRaw ? JSON.parse(_visHiddenColsRaw) : _VIS_DEFAULT_HIDDEN);
let _visColResizeDone = false;
let _visSortCol = 'visited_at';
let _visSortDir = 'desc';

// Visual-only pagination for visits — same pattern as bookmarks
const _VIS_PAGE_SIZE_OPTIONS = [25, 50, 100, 250, 1000];
let _visPage = 1;
let _visPageSize = (() => {
  const n = parseInt(localStorage.getItem('vis-page-size') || '100', 10);
  return _VIS_PAGE_SIZE_OPTIONS.includes(n) ? n : 100;
})();

// Domain grouping — active when sorted by url or domain
let _visExpandedDomains = new Set(); // domains currently expanded in group mode

function _visVisibleCols() { return _VIS_ALL_COLS.filter(k => !_visHiddenCols.has(k)); }
function _visColCount()    { return _visVisibleCols().length; }

// Return the hostname (stripped of www.) for a URL string
function _visDomain(url) {
  try { return new URL(url).hostname.replace(/^www\./, ''); }
  catch (_) { return ''; }
}

// Return the first path segment of a URL, e.g. '/docs', or '/' for root
function _visFirstSlug(url) {
  try {
    const parts = new URL(url).pathname.split('/').filter(Boolean);
    return parts.length ? '/' + parts[0] : '/';
  } catch (_) { return '/'; }
}

function _visRebuildThead() {
  const tr = document.getElementById('vis-thead-row');
  if (!tr) return;
  tr.innerHTML = _visVisibleCols().map(k => {
    const label   = _VIS_FIELD_META[k]?.label ?? k;
    const sortKey = _VIS_SORT_KEYS[k] ?? null;
    const style   = k === '_actions' ? ' style="width:90px"' : k === 'visit_count' ? ' style="width:50px;text-align:center"' : '';
    return sortKey
      ? `<th class="bm-th-sort" onclick="_visSortBy('${sortKey}')"${style}>${label}<span class="bm-sort-arrow vis-sort-arrow" data-col="${sortKey}">&#x21C5;</span></th>`
      : `<th${style}>${label}</th>`;
  }).join('');
  _visColResizeDone = false;
  _visUpdateSortHeaders();
}

function _visInitColResize() {
  if (_visColResizeDone) return;
  const table = document.getElementById('vis-table');
  if (!table) return;
  _visColResizeDone = true;
  table.querySelectorAll('thead th').forEach(th => {
    const resizer = document.createElement('div');
    resizer.className = 'bm-col-resize';
    th.appendChild(resizer);
    resizer.addEventListener('click', e => { e.preventDefault(); e.stopPropagation(); });
    let startX = 0, startW = 0;
    resizer.addEventListener('mousedown', e => {
      e.preventDefault(); e.stopPropagation();
      startX = e.clientX; startW = th.offsetWidth;
      resizer.classList.add('dragging');
      const onMove = ev => { th.style.width = Math.max(40, startW + ev.clientX - startX) + 'px'; };
      const onUp   = () => { resizer.classList.remove('dragging'); document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  });
}

function _visOpenColsModal() {
  const list = document.getElementById('vis-cols-modal-list');
  list.innerHTML = _VIS_ALL_COLS.map(k => {
    const label   = _VIS_FIELD_META[k]?.label ?? k;
    const checked = !_visHiddenCols.has(k) ? 'checked' : '';
    return `<label style="display:flex;align-items:center;gap:10px;cursor:pointer;padding:8px 4px;font-size:13px;border-bottom:1px solid var(--border)">
      <input type="checkbox" data-col="${k}" ${checked} style="width:15px;height:15px;cursor:pointer" />
      <span>${label}</span>
    </label>`;
  }).join('');
  document.getElementById('vis-cols-modal').showModal();
}

function _visApplyColsModal() {
  const modal = document.getElementById('vis-cols-modal');
  const newHidden = new Set(_visHiddenCols);
  modal.querySelectorAll('input[data-col]').forEach(cb => {
    if (cb.checked) newHidden.delete(cb.dataset.col);
    else            newHidden.add(cb.dataset.col);
  });
  _visHiddenCols = newHidden;
  localStorage.setItem('vis-hidden-cols', JSON.stringify([..._visHiddenCols]));
  _visRebuildThead();
  renderVisits({ keepPage: true }); // column toggle — stay on current page
  modal.close();
}

async function loadVisits() {
  const err = document.getElementById('bm-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/bookmarks/visits?limit=1000');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _bmVisits = await r.json();
    _visRebuildThead();
    renderVisits();
  } catch (e) {
    err.textContent = `Failed to load visits: ${e.message}`;
    err.hidden = false;
  }
}

function renderVisits(opts = {}) {
  if (!opts.keepPage) _visPage = 1;
  const q = (document.getElementById('bm-visit-search')?.value || '').toLowerCase();
  const savedFilter = document.getElementById('bm-visit-saved-filter')?.value || 'all';
  let rows = _bmVisits;
  if (q) {
    rows = rows.filter(v =>
      (v.title || '').toLowerCase().includes(q) ||
      (v.url || '').toLowerCase().includes(q) ||
      (v.domain || '').toLowerCase().includes(q)
    );
  }
  if (savedFilter === 'saved')   rows = rows.filter(v => v.bookmark_id);
  if (savedFilter === 'unsaved') rows = rows.filter(v => !v.bookmark_id);
  // Sort
  rows = [...rows].sort((a, b) => {
    const av = String(a[_visSortCol] ?? '').toLowerCase();
    const bv = String(b[_visSortCol] ?? '').toLowerCase();
    return _visSortDir === 'asc'
      ? av.localeCompare(bv, undefined, {numeric: true})
      : bv.localeCompare(av, undefined, {numeric: true});
  });

  const groupMode = (_visSortCol === 'url' || _visSortCol === 'domain');
  // Show/hide grouping controls
  const expandBtn   = document.getElementById('vis-expand-all-btn');
  const collapseBtn = document.getElementById('vis-collapse-all-btn');
  if (expandBtn)   expandBtn.hidden   = !groupMode;
  if (collapseBtn) collapseBtn.hidden = !groupMode;

  const cols  = _visVisibleCols();
  const tbody = document.getElementById('bm-visits-tbody');
  const status = document.getElementById('vis-status');

  if (!groupMode) {
    // ── Non-group mode: paginate visit rows ──────────────────────────────
    const total = rows.length;
    const totalPages = Math.max(1, Math.ceil(total / _visPageSize));
    _visPage = Math.max(1, Math.min(_visPage, totalPages));
    const pageRows = rows.slice((_visPage - 1) * _visPageSize, _visPage * _visPageSize);
    if (status) {
      if (total > _visPageSize) {
        const from = (_visPage - 1) * _visPageSize + 1;
        const to   = Math.min(_visPage * _visPageSize, total);
        status.textContent = `${total} visit${total === 1 ? '' : 's'} (showing ${from}\u2013${to})`;
      } else {
        status.textContent = `${total} visit${total === 1 ? '' : 's'}`;
      }
      status.hidden = false;
    }
    if (!pageRows.length) {
      tbody.innerHTML = `<tr class="empty-row"><td colspan="${cols.length}">No visit history.</td></tr>`;
      _visUpdateSortHeaders();
      _visRenderPagination(total, _visPageSize, _visPage);
      return;
    }
    const expandColspan = cols.length;
    tbody.innerHTML = pageRows.map(v => {
      const expandId = `ve-${esc(v.visit_id)}`;
      const tds = cols.map(k => (_VIS_FIELD_META[k]?.render ?? (v => `<td>${esc(String(v[k] ?? ''))}</td>`))(v)).join('');
      return `<tr>${tds}</tr>
      <tr id="${expandId}" style="display:none">
        <td colspan="${expandColspan}" style="padding:0 0 6px 18px">
          <div id="${expandId}-body" style="font-size:11px;color:var(--text-dim)">Loading&hellip;</div>
        </td>
      </tr>`;
    }).join('');
    _visInitColResize();
    _visUpdateSortHeaders();
    _visRenderPagination(total, _visPageSize, _visPage);
    return;
  }

  // ── Group mode: group by domain, sub-group by first path segment ──────
  const domainOrder = [];
  const domainGroups = new Map();
  for (const v of rows) {
    const domain = v.domain || _visDomain(v.url || '') || '(unknown)';
    if (!domainGroups.has(domain)) {
      domainGroups.set(domain, []);
      domainOrder.push(domain);
    }
    domainGroups.get(domain).push(v);
  }

  // Build flat list of visible items (domain headers + optional slug headers + visit rows)
  const items = [];
  for (const domain of domainOrder) {
    const visits  = domainGroups.get(domain);
    const expanded = _visExpandedDomains.has(domain);
    const totalVisitCount = visits.reduce((s, v) => s + (v.visit_count || 1), 0);
    items.push({ type: 'domain-header', domain, urlCount: visits.length, totalVisitCount, expanded });
    if (expanded) {
      // Only show slug sub-headers when there are multiple different first segments
      const slugSet = new Set(visits.map(v => _visFirstSlug(v.url || '')));
      const hasMultipleSlugs = slugSet.size > 1;
      let lastSlug = null;
      for (const v of visits) {
        const slug = _visFirstSlug(v.url || '');
        if (hasMultipleSlugs && slug !== lastSlug) {
          items.push({ type: 'slug-header', slug });
          lastSlug = slug;
        }
        items.push({ type: 'visit', v });
      }
    }
  }

  // Paginate over flat items
  const totalItems = items.length;
  const totalPages = Math.max(1, Math.ceil(totalItems / _visPageSize));
  _visPage = Math.max(1, Math.min(_visPage, totalPages));
  const pageItems = items.slice((_visPage - 1) * _visPageSize, _visPage * _visPageSize);

  if (status) {
    const totalDomains = domainOrder.length;
    const totalVisits  = rows.reduce((s, v) => s + (v.visit_count || 1), 0);
    const totalUrls    = rows.length;
    const expandedCount = [..._visExpandedDomains].filter(d => domainGroups.has(d)).length;
    let statusText = `${totalVisits} visit${totalVisits === 1 ? '' : 's'} (${totalUrls} URL${totalUrls === 1 ? '' : 's'}) across ${totalDomains} domain${totalDomains === 1 ? '' : 's'}`;
    if (expandedCount) statusText += ` (${expandedCount} expanded)`;
    if (totalItems > _visPageSize) {
      const from = (_visPage - 1) * _visPageSize + 1;
      const to   = Math.min(_visPage * _visPageSize, totalItems);
      statusText += ` — rows ${from}\u2013${to} of ${totalItems}`;
    }
    status.textContent = statusText;
    status.hidden = false;
  }

  if (!pageItems.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="${cols.length}">No visit history.</td></tr>`;
    _visUpdateSortHeaders();
    _visRenderPagination(totalItems, _visPageSize, _visPage);
    return;
  }

  const expandColspan = cols.length;
  let html = '';
  for (const item of pageItems) {
    if (item.type === 'domain-header') {
      const chevron = item.expanded ? '&#9660;' : '&#9658;';
      html += `<tr class="vis-group-header" onclick="_visToggleDomain('${esc(item.domain)}')"
        style="cursor:pointer;background:rgba(0,0,0,0.25)">
        <td colspan="${expandColspan}" style="padding:7px 10px;font-weight:600;font-size:12px;user-select:none">
          <span style="color:var(--text-dim);margin-right:6px;font-size:11px">${chevron}</span>
          <span style="color:var(--accent)">${esc(item.domain)}</span>
          <span style="color:var(--text-dim);font-weight:400;margin-left:8px;font-size:11px">${item.totalVisitCount} visit${item.totalVisitCount === 1 ? '' : 's'}</span>
          ${item.urlCount > 1 ? `<span style="color:var(--text-dim);font-weight:400;font-size:10px;margin-left:5px">(${item.urlCount} URLs)</span>` : ''}
        </td>
      </tr>`;
    } else if (item.type === 'slug-header') {
      html += `<tr class="vis-slug-header">
        <td colspan="${expandColspan}" style="padding:3px 10px 3px 28px;font-size:11px;color:var(--text-dim);border-top:1px solid rgba(255,255,255,0.06);background:rgba(0,0,0,0.12);font-style:italic">${esc(item.slug)}</td>
      </tr>`;
    } else { // type === 'visit'
      const v = item.v;
      const expandId = `ve-${esc(v.visit_id)}`;
      const tds = cols.map(k => (_VIS_FIELD_META[k]?.render ?? (v => `<td>${esc(String(v[k] ?? ''))}</td>`))(v)).join('');
      html += `<tr>${tds}</tr>
      <tr id="${expandId}" style="display:none">
        <td colspan="${expandColspan}" style="padding:0 0 6px 18px">
          <div id="${expandId}-body" style="font-size:11px;color:var(--text-dim)">Loading&hellip;</div>
        </td>
      </tr>`;
    }
  }
  tbody.innerHTML = html;
  _visInitColResize();
  _visUpdateSortHeaders();
  _visRenderPagination(totalItems, _visPageSize, _visPage);
}

function _visSortBy(col) {
  const wasGroupMode = (_visSortCol === 'url' || _visSortCol === 'domain');
  if (_visSortCol === col) {
    _visSortDir = _visSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    _visSortCol = col;
    _visSortDir = 'asc';
  }
  const nowGroupMode = (_visSortCol === 'url' || _visSortCol === 'domain');
  // When entering group mode fresh, start with all groups collapsed
  if (!wasGroupMode && nowGroupMode) _visExpandedDomains = new Set();
  renderVisits();
}

// Toggle a single domain group open/closed
function _visToggleDomain(domain) {
  if (_visExpandedDomains.has(domain)) {
    _visExpandedDomains.delete(domain);
  } else {
    _visExpandedDomains.add(domain);
  }
  renderVisits({ keepPage: true });
}

// Expand or collapse all domain groups
function _visSetAllDomains(expanded) {
  if (expanded) {
    _visExpandedDomains = new Set(
      _bmVisits.map(v => v.domain || _visDomain(v.url || '') || '(unknown)')
    );
  } else {
    _visExpandedDomains = new Set();
  }
  _visPage = 1;
  renderVisits({ keepPage: true });
}

// Pagination controls
function _visRenderPagination(total, pageSize, page) {
  const el = document.getElementById('vis-pagination');
  if (!el) return;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const sizeOpts = _VIS_PAGE_SIZE_OPTIONS.map(n =>
    `<option value="${n}"${n === pageSize ? ' selected' : ''}>${n}</option>`
  ).join('');
  const sizeSelect = `<label style="display:flex;align-items:center;gap:6px;cursor:pointer">
    Per page:
    <select onchange="_visSetPageSize(parseInt(this.value,10))" style="font-size:12px;padding:2px 6px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text)">${sizeOpts}</select>
  </label>`;
  if (totalPages <= 1) {
    // Always show the per-page selector even when everything fits on one page
    el.innerHTML = `<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:12px;color:var(--text-dim);padding:8px 0 4px">${sizeSelect}</div>`;
    return;
  }
  const prevDis = page <= 1 ? ' disabled' : '';
  const nextDis = page >= totalPages ? ' disabled' : '';
  el.innerHTML = `
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:12px;color:var(--text-dim);padding:8px 0 4px">
      <button class="secondary" style="padding:2px 10px;font-size:12px"${prevDis} onclick="_visGoPage(${page - 1})">&#8592; Prev</button>
      <span>Page <strong style="color:var(--text)">${page}</strong> of <strong style="color:var(--text)">${totalPages}</strong></span>
      <button class="secondary" style="padding:2px 10px;font-size:12px"${nextDis} onclick="_visGoPage(${page + 1})">Next &#8594;</button>
      ${sizeSelect}
    </div>`;
}

function _visGoPage(n) {
  _visPage = n;
  renderVisits({ keepPage: true });
}

function _visSetPageSize(n) {
  if (!_VIS_PAGE_SIZE_OPTIONS.includes(n)) return;
  _visPageSize = n;
  localStorage.setItem('vis-page-size', String(n));
  _visPage = 1;
  renderVisits({ keepPage: true });
}

function _visUpdateSortHeaders() {
  document.querySelectorAll('.vis-sort-arrow').forEach(span => {
    const col = span.dataset.col;
    if (col === _visSortCol) {
      span.textContent = _visSortDir === 'asc' ? ' ↑' : ' ↓';
      span.classList.add('active');
    } else {
      span.textContent = '⇅';
      span.classList.remove('active');
    }
  });
}

async function _bmToggleVisitEvents(normalizedUrl, rowId) {
  const row = document.getElementById(rowId);
  if (!row) return;
  const body = document.getElementById(rowId + '-body');
  if (row.style.display !== 'none') { row.style.display = 'none'; return; }
  row.style.display = '';
  if (body.dataset.loaded) return;
  const fmtDateTime = iso => {
    if (!iso) return '—';
    try {
      return new Date(iso).toLocaleString(undefined, {
        year: 'numeric', month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit', second: '2-digit'
      });
    } catch (_) { return iso; }
  };
  try {
    const r = await apiFetch('/api/v1/bookmarks/visit-events?normalized_url=' + encodeURIComponent(normalizedUrl));
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const events = await r.json();
    body.dataset.loaded = '1';
    if (!events.length) { body.textContent = 'No events recorded.'; return; }
    body.innerHTML = '<table style="border-collapse:collapse;width:100%"><thead><tr>'
      + '<th style="text-align:left;padding:2px 8px;color:var(--text-dim);font-weight:600">Date / Time</th>'
      + '<th style="text-align:left;padding:2px 8px;color:var(--text-dim);font-weight:600">Dwell</th>'
      + '</tr></thead><tbody>'
      + events.map(e => {
          const d = e.dwell_seconds ? `${e.dwell_seconds}s` : '—';
          return `<tr><td style="padding:2px 8px">${esc(fmtDateTime(e.visited_at))}</td><td style="padding:2px 8px">${d}</td></tr>`;
        }).join('')
      + '</tbody></table>';
  } catch(err) {
    body.textContent = `Failed: ${err.message}`;
  }
}

function _bmToggleVisits() {
  switchTab('bookmarks-history');
  if (typeof ProbesMenuConfig !== 'undefined') ProbesMenuConfig.updateActiveTab('bookmarks-history');
  loadVisits();
}

function _bmToggleSetup() {
  switchTab('bookmarks-setup');
  if (typeof ProbesMenuConfig !== 'undefined') ProbesMenuConfig.updateActiveTab('bookmarks-setup');
  _bmPopulateExtUrls();
}

let _bmReindexPollTimer = null;

function _bmToggleEmbedCfg() {
  switchTab('bookmarks-embeddings');
  if (typeof ProbesMenuConfig !== 'undefined') ProbesMenuConfig.updateActiveTab('bookmarks-embeddings');
  _bmLoadEmbedCfg();
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
  // bm-embed-panel is now a permanent section (tab-bookmarks-embeddings).
  // No close button — close btn wiring is no longer needed.
  // Wire the remaining interactive controls.

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
    renderBookmarks({ keepPage: true }); // stay on current page after deleting one row
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
  if (_bmSearchActive) {
    _bmLastSearchResults = [..._bmLastSearchResults].sort((a, b) => {
      const av = _bmSortVal(a, col);
      const bv = _bmSortVal(b, col);
      return _bmSortDir === 'asc' ? av.localeCompare(bv, undefined, {numeric: true}) : bv.localeCompare(av, undefined, {numeric: true});
    });
    _renderBmSearchResults(_bmLastSearchResults);
  } else {
    renderBookmarks();
  }
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
  // btn may be null when called from the navbar menu rather than the toolbar button
  if (btn) btn.disabled = true;
  const orig = btn ? btn.textContent : '';
  if (btn) btn.textContent = '\u27F3 Checking\u2026';
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
    if (btn) { btn.disabled = false; btn.textContent = orig; }
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

// ── Score analysis modal ─────────────────────────────────────────────────

const _BM_SCORE_METRICS = ['score_sources', 'rrf_score', 'kw_tier', 'cosine_distance', 'reranker_rank', 'exact_tier'];

const _BM_METRIC_LABELS = {
  score_sources:    'Score Sources',
  rrf_score:        'RRF Score',
  kw_tier:          'Keyword Tier',
  cosine_distance:  'Cosine Distance',
  reranker_rank:    'Reranker Rank',
  exact_tier:       'Exact Tier',
};

// Stored context for the currently-open overview modal — used by drill-down delegation.
let _bmScoreCtx = { query: '', result: null };

// Called when any .bm-score-cell is clicked (event-delegated from bm-tbody).
function _bmOpenScoreModal(cell) {
  const tr = cell.closest('tr[data-score-idx]');
  if (!tr) return;
  const idx = parseInt(tr.dataset.scoreIdx, 10);
  const result = _bmLastSearchResults[idx];
  if (!result) return;
  const query = (document.getElementById('bm-search')?.value || '').trim();
  _bmScoreCtx = { query, result };
  const title = result.title || result.url || 'result';
  document.getElementById('bm-score-modal-subtitle').textContent = `"${title.slice(0, 70)}"`;
  const body = document.getElementById('bm-score-modal-body');
  body.innerHTML = _bmScoreLoadingHtml();
  document.getElementById('bm-score-modal').showModal();
  _bmFetchScoreExplain(query, result, null, body);
}

// Called when a per-metric drill-down link is clicked inside the overview modal.
// Uses _bmScoreCtx so no inline JSON is needed.
function _bmOpenScoreDetailModal(metric) {
  const { query, result } = _bmScoreCtx;
  if (!result) return;
  const label = _BM_METRIC_LABELS[metric] || metric;
  document.getElementById('bm-score-detail-subtitle').textContent = label;
  const body = document.getElementById('bm-score-detail-body');
  body.innerHTML = _bmScoreLoadingHtml();
  document.getElementById('bm-score-detail-modal').showModal();
  _bmFetchScoreExplain(query, result, metric, body);
}

function _bmScoreLoadingHtml() {
  return `<div style="display:flex;align-items:center;gap:10px;color:var(--text-dim);padding:20px 0">
    <span style="font-size:18px;animation:spin 1s linear infinite;display:inline-block">&#8635;</span>
    <span>Asking LLM…</span>
  </div>`;
}

async function _bmFetchScoreExplain(query, result, focus, bodyEl) {
  try {
    const r = await apiFetch('/api/v1/bookmarks/score-explain', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, result, focus }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const md = data.explanation || '(No response)';
    let html = typeof _mdToHtml === 'function' ? _mdToHtml(md) : `<pre style="white-space:pre-wrap">${esc(md)}</pre>`;
    if (!focus) {
      // Drill-down links use data-metric and event delegation — no inline JSON
      const metricLinksHtml = _BM_SCORE_METRICS
        .filter(m => result[m] != null)
        .map(m => {
          const label = _BM_METRIC_LABELS[m] || m;
          const val = Array.isArray(result[m]) ? result[m].join(', ') : String(result[m]);
          return `<a href="#" class="bm-score-drill" data-metric="${esc(m)}"
                     style="display:inline-flex;align-items:center;gap:4px;color:var(--accent);font-size:12px;text-decoration:underline;white-space:nowrap"
                  >&#128270; ${esc(label)} <span style="color:var(--text-dim);font-size:11px">(${esc(val.slice(0,30))})</span></a>`;
        }).join('');
      if (metricLinksHtml) {
        html += `<div style="margin-top:24px;padding-top:14px;border-top:1px solid var(--border)">
          <div style="font-size:12px;font-weight:600;color:var(--text-dim);margin-bottom:10px;text-transform:uppercase;letter-spacing:.05em">Drill into a metric</div>
          <div style="display:flex;flex-wrap:wrap;gap:8px">${metricLinksHtml}</div>
        </div>`;
      }
    }
    bodyEl.innerHTML = html;
  } catch (e) {
    bodyEl.innerHTML = `<p style="color:var(--err)">Error: ${esc(e.message)}</p>`;
  }
}

// Event delegation: score cells in results table + drill-down links in overview modal
document.addEventListener('DOMContentLoaded', () => {
  // Wire the search/filter controls that now live in #pg-ctrl-bookmarks-main
  // (moved from the tab-panel toolbar into the menu-zone page-controls slot).
  const bmSearch   = document.getElementById('bm-search');
  const bmTagFilt  = document.getElementById('bm-tag-filter');
  const bmArchived = document.getElementById('bm-show-archived');
  if (bmSearch)   bmSearch.addEventListener('input', _bmSearchDebounce);
  if (bmTagFilt)  bmTagFilt.addEventListener('change', () => {
    _bmSearchActive ? _renderBmSearchResults(_bmLastSearchResults) : renderBookmarks();
  });
  if (bmArchived) bmArchived.addEventListener('change', loadBookmarks);

  // Replace native tag-filter select with custom fixed-popup dropdown.
  // Must come AFTER the change listener above so the listener is already
  // registered when HubSelect fires synthetic change events.
  if (typeof HubSelect !== 'undefined') {
    HubSelect.init('bm-tag-filter');
  }

  // Register with responsive layout so controls show/hide on tab switch
  if (typeof ResponsiveLayout !== 'undefined') {
    ResponsiveLayout.registerTabControls('bookmarks-main', 'pg-ctrl-bookmarks-main');
  }

  document.getElementById('bm-tbody')?.addEventListener('click', e => {
    const cell = e.target.closest('.bm-score-cell');
    if (cell) _bmOpenScoreModal(cell);
  });
  document.getElementById('bm-score-modal-body')?.addEventListener('click', e => {
    const link = e.target.closest('.bm-score-drill');
    if (link) {
      e.preventDefault();
      _bmOpenScoreDetailModal(link.dataset.metric);
    }
  });
});

// ── Sort explanation modal ───────────────────────────────────────────────

async function _bmOpenSortExplainModal() {
  if (!_bmSearchActive || !_bmLastSearchResults.length) return;
  const query = (document.getElementById('bm-search')?.value || '').trim();
  const top = _bmLastSearchResults.slice(0, 20);
  const subtitle = document.getElementById('bm-sort-explain-subtitle');
  subtitle.textContent = `"${query}" — top ${top.length} results`;
  const body = document.getElementById('bm-sort-explain-body');
  body.innerHTML = _bmScoreLoadingHtml();
  document.getElementById('bm-sort-explain-modal').showModal();
  try {
    const r = await apiFetch('/api/v1/bookmarks/sort-explain', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query,
        results: top,
        sort_col: _bmSortCol || 'compound',
        sort_dir: _bmSortDir || 'asc',
      }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const md = data.explanation || '(No response)';
    body.innerHTML = typeof _mdToHtml === 'function' ? _mdToHtml(md) : `<pre style="white-space:pre-wrap">${esc(md)}</pre>`;
  } catch (e) {
    body.innerHTML = `<p style="color:var(--err)">Error: ${esc(e.message)}</p>`;
  }
}
