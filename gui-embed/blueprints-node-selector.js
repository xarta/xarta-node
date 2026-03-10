/**
 * blueprints-node-selector.js — Phase 2 (dynamic /api/v1/nodes discovery)
 *
 * Standalone web-component dropdown showing the current healthy Blueprints node.
 * Discovers the live node list from the API; health-checks every 10 s;
 * fails over automatically. No build step, no dependencies.
 *
 * Configuration (set window globals BEFORE this script loads):
 *
 *   window.BLUEPRINTS_API_BASE    — base URL of any live Blueprints node, used
 *                                   to fetch the node list. Default: current origin.
 *
 *   window.BLUEPRINTS_SEED_NODES  — [{id, name, url}] static fallback / extras.
 *                                   Merged with API-discovered nodes by id.
 *
 * Embedding in another service:
 *   <script>
 *     window.BLUEPRINTS_API_BASE = 'https://my-node.my-tailnet.ts.net';
 *   </script>
 *   <link  rel="stylesheet" href="https://my-node.my-tailnet.ts.net/ui/embed/blueprints-node-selector.css" />
 *   <script src="https://my-node.my-tailnet.ts.net/ui/embed/blueprints-node-selector.js"></script>
 *   <blueprints-node-selector></blueprints-node-selector>
 */
(function () {
  'use strict';

  // ── Configuration ──────────────────────────────────────────────────────────
  const API_BASE = (
    (typeof window !== 'undefined' && window.BLUEPRINTS_API_BASE) || ''
  ).replace(/\/$/, '');

  const SEEDS = (typeof window !== 'undefined' && window.BLUEPRINTS_SEED_NODES) || [];

  const LS_NODES   = 'bp_nodes_v2';
  const LS_CURRENT = 'bp_current_v2';

  const HEALTH_INTERVAL = 10_000;  // health-check cadence for current node (ms)
  const DOWN_RETRY      = 60_000;  // retry interval for unavailable nodes (ms)
  const LIST_REFRESH    = 60_000;  // node-list refresh from API (ms)
  const LS_TTL          = 5 * 60_000; // localStorage cache TTL (ms)

  // ── State ──────────────────────────────────────────────────────────────────
  // node shape: { id, name, uiUrl, healthUrl, available, lastChecked }
  //   uiUrl     — where the browser navigates (from /health ui_url field)
  //   healthUrl — where health-checks are sent (sync address /health)
  let _nodes   = [];
  let _current = null;  // id of the active node

  // ── localStorage helpers ───────────────────────────────────────────────────
  function lsGet(key)      { try { return JSON.parse(localStorage.getItem(key)); } catch { return null; } }
  function lsSet(key, val) { try { localStorage.setItem(key, JSON.stringify(val)); } catch {} }

  // ── Node list refresh from API ─────────────────────────────────────────────
  async function refreshNodeList() {
    const origin = API_BASE || window.location.origin;

    // 1. Self node — call /health on same origin (no CORS issue)
    let selfNode = null;
    try {
      const r = await fetch(`${origin}/health`, { signal: AbortSignal.timeout(5000) });
      if (r.ok) {
        const h = await r.json();
        selfNode = {
          id:        h.node_id,
          name:      h.node_name || h.node_id,
          uiUrl:     (h.ui_url || origin).replace(/\/$/, ''),
          healthUrl: `${origin}/health`,
        };
      }
    } catch { /* keep selfNode null */ }

    // 2. Peer nodes from API
    let peers = [];
    try {
      const r = await fetch(`${origin}/api/v1/nodes`, { signal: AbortSignal.timeout(5000) });
      if (r.ok) peers = await r.json();
    } catch { /* keep peers empty */ }

    // 3. Build fresh list — self first, then peers
    const fresh = [];
    if (selfNode) fresh.push(selfNode);

    for (const p of peers) {
      const syncAddr = (p.addresses && p.addresses[0])
        ? p.addresses[0].replace(/\/$/, '') : null;
      if (!syncAddr) continue;
      // Skip if already in list (e.g. API returned self node)
      if (fresh.find(n => n.id === p.node_id)) continue;

      // Use ui_url from node record — avoids mixed-content http calls from HTTPS page.
      // Fall back to a same-scheme health fetch only if ui_url absent (legacy nodes).
      let uiUrl = p.ui_url ? p.ui_url.replace(/\/$/, '') : null;
      if (!uiUrl) {
        const isSameScheme = syncAddr.startsWith(window.location.protocol);
        if (isSameScheme) {
          try {
            const hResp = await fetch(`${syncAddr}/health`, { signal: AbortSignal.timeout(4000) });
            if (hResp.ok) {
              const hj = await hResp.json();
              if (hj.ui_url) uiUrl = hj.ui_url.replace(/\/$/, '');
            }
          } catch { /* fall through */ }
        }
        if (!uiUrl) uiUrl = syncAddr;
      }

      fresh.push({
        id:        p.node_id,
        name:      p.display_name || p.node_id,
        uiUrl:     uiUrl,
        healthUrl: `${uiUrl}/health`,
      });
    }

    // 4. Merge seed nodes (include seeds not returned by the API)
    for (const s of SEEDS) {
      if (!fresh.find(n => n.id === s.id)) {
        fresh.push({
          id:        s.id,
          name:      s.name,
          uiUrl:     (s.url || '').replace(/\/$/, ''),
          healthUrl: `${(s.url || '').replace(/\/$/, '')}/health`,
        });
      }
    }

    if (!fresh.length) return; // no data — keep existing list

    // 5. Merge with existing (preserve availability flags)
    const byId = Object.fromEntries(_nodes.map(n => [n.id, n]));
    _nodes = fresh.map(n => Object.assign(
      { available: true, lastChecked: 0 },
      n,
      byId[n.id] ? { available: byId[n.id].available, lastChecked: byId[n.id].lastChecked } : {},
    ));

    // 6. Ensure _current is valid; default to self or first node
    if (!_current || !_nodes.find(n => n.id === _current)) {
      _current = (selfNode && selfNode.id) || (_nodes[0] && _nodes[0].id) || null;
    }

    lsSet(LS_NODES,   { ts: Date.now(), nodes: _nodes });
    lsSet(LS_CURRENT, _current);
    renderBtn();
    renderPanel();
  }

  // ── Health checking ────────────────────────────────────────────────────────
  async function pingNode(node) {
    try {
      const r = await fetch(node.healthUrl, {
        method: 'GET',
        signal: AbortSignal.timeout(5000),
      });
      return r.ok;
    } catch { return false; }
  }

  async function checkCurrent() {
    const node = _nodes.find(n => n.id === _current);
    if (!node) { await electNew(); return; }

    setDot('checking');
    const ok = await pingNode(node);
    node.available = ok;
    node.lastChecked = Date.now();
    lsSet(LS_NODES, { ts: Date.now(), nodes: _nodes });

    if (ok) { setDot('ok'); }
    else    { setDot('down'); await electNew(); }
  }

  async function electNew() {
    for (const node of _nodes) {
      if (node.id === _current) continue;
      if (!node.available && (Date.now() - node.lastChecked) < DOWN_RETRY) continue;
      setDot('checking');
      const ok = await pingNode(node);
      node.available = ok;
      node.lastChecked = Date.now();
      if (ok) {
        _current = node.id;
        lsSet(LS_CURRENT, _current);
        lsSet(LS_NODES, { ts: Date.now(), nodes: _nodes });
        setDot('ok');
        renderBtn();
        renderPanel();
        return;
      }
    }
    setDot('down');
    lsSet(LS_NODES, { ts: Date.now(), nodes: _nodes });
  }

  function recheckDown() {
    const now = Date.now();
    _nodes.forEach(n => {
      if (!n.available && (now - n.lastChecked) >= DOWN_RETRY) {
        pingNode(n).then(ok => {
          n.available = ok;
          n.lastChecked = Date.now();
          if (ok && !isCurrentOk()) {
            _current = n.id;
            lsSet(LS_CURRENT, _current);
            setDot('ok');
            renderBtn();
          }
          lsSet(LS_NODES, { ts: Date.now(), nodes: _nodes });
          renderPanel();
        });
      }
    });
  }

  function isCurrentOk() {
    const n = _nodes.find(n => n.id === _current);
    return n ? n.available : false;
  }

  // ── Bootstrap ──────────────────────────────────────────────────────────────
  function init() {
    // Warm-start from cache if fresh
    const cached = lsGet(LS_NODES);
    if (cached && cached.nodes && (Date.now() - cached.ts) < LS_TTL) {
      _nodes   = cached.nodes;
      _current = lsGet(LS_CURRENT) || (_nodes[0] && _nodes[0].id) || null;
    } else if (SEEDS.length) {
      _nodes = SEEDS.map(s => ({
        id: s.id, name: s.name,
        uiUrl:     (s.url || '').replace(/\/$/, ''),
        healthUrl: `${(s.url || '').replace(/\/$/, '')}/health`,
        available: true, lastChecked: 0,
      }));
      _current = _nodes[0] && _nodes[0].id;
    }

    renderBtn();
    renderPanel();

    // Non-blocking live discovery
    refreshNodeList();
    checkCurrent().then(() => renderPanel());

    setInterval(() => checkCurrent().then(() => renderPanel()), HEALTH_INTERVAL);
    setInterval(recheckDown, 15_000);
    setInterval(refreshNodeList, LIST_REFRESH);
  }

  // ── Custom element ─────────────────────────────────────────────────────────
  class BlueprintsNodeSelector extends HTMLElement {
    connectedCallback() {
      this.innerHTML = `
        <div class="bp-node-selector">
          <button class="bp-ns-btn" id="bp-ns-toggle"
                  aria-haspopup="true" aria-expanded="false">
            <span class="bp-ns-dot"  id="bp-ns-dot"></span>
            <span class="bp-ns-name" id="bp-ns-name">…</span>
            <span class="bp-ns-caret">▾</span>
          </button>
          <div class="bp-ns-panel" id="bp-ns-panel">
            <div class="bp-ns-header">Blueprints nodes</div>
            <div id="bp-ns-list"></div>
          </div>
        </div>`;

      this.querySelector('#bp-ns-toggle').addEventListener('click', e => {
        e.stopPropagation();
        togglePanel();
      });
      document.addEventListener('click', closePanel);
      init();
    }
  }

  // ── Rendering ──────────────────────────────────────────────────────────────
  function renderBtn() {
    const el = document.getElementById('bp-ns-name');
    if (!el) return;
    const node = _nodes.find(n => n.id === _current);
    el.textContent = node ? node.name : 'No node';
  }

  function setDot(state) {
    const el = document.getElementById('bp-ns-dot');
    if (el) el.className = 'bp-ns-dot' + (state === 'ok' ? '' : ` ${state}`);
  }

  function renderPanel() {
    const list = document.getElementById('bp-ns-list');
    if (!list) return;
    if (!_nodes.length) {
      list.innerHTML = '<div class="bp-ns-node" style="color:#5b6080">Discovering nodes…</div>';
      return;
    }
    list.innerHTML = _nodes.map(n => `
      <div class="bp-ns-node${n.id === _current ? ' active' : ''}${!n.available ? ' bp-ns-node-unavail' : ''}"
           data-id="${esc(n.id)}" data-url="${esc(n.uiUrl)}">
        <span class="bp-ns-dot${n.available ? '' : ' down'}"></span>
        <span class="bp-ns-node-name">${esc(n.name)}</span>
        <span class="bp-ns-node-url">${esc(n.uiUrl)}</span>
      </div>`).join('');

    list.querySelectorAll('.bp-ns-node').forEach(el => {
      el.addEventListener('click', e => {
        e.stopPropagation();
        _current = el.dataset.id;
        lsSet(LS_CURRENT, _current);
        closePanel();
        renderBtn();
        renderPanel();
        window.location.href = `${el.dataset.url}/ui/`;
      });
    });
  }

  function togglePanel() {
    const p = document.getElementById('bp-ns-panel');
    if (!p) return;
    const open = p.classList.toggle('open');
    const btn  = document.getElementById('bp-ns-toggle');
    if (btn) btn.setAttribute('aria-expanded', String(open));
    if (open) renderPanel();
  }

  function closePanel() {
    const p = document.getElementById('bp-ns-panel');
    if (p) p.classList.remove('open');
  }

  function esc(s) {
    return String(s).replace(/[&<>"']/g,
      c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  // ── Register custom element ────────────────────────────────────────────────
  if (!customElements.get('blueprints-node-selector')) {
    customElements.define('blueprints-node-selector', BlueprintsNodeSelector);
  }
})();
