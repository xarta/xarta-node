/**
 * blueprints-node-selector.js — dynamic node discovery + configurable nav buttons.
 *
 * Configuration (set window globals BEFORE this script loads):
 *
 *   window.BLUEPRINTS_API_BASE
 *   window.BLUEPRINTS_SEED_NODES
 *   window.BLUEPRINTS_SELECTOR_BUTTONS = {
 *     enabledButtons: ['ui', 'fallback-ui', 'database-tables', 'database-diagram', 'paging-button'],
 *     side: 'left' | 'right',
 *     pageSize: 4,
 *     nodeSwitchPath: '/ui/' | 'current'
 *   }
 */
(function () {
  'use strict';

  const SCRIPT_SRC = (typeof document !== 'undefined' && document.currentScript && document.currentScript.src)
    ? document.currentScript.src
    : '';
  const SCRIPT_DIR = SCRIPT_SRC.includes('/')
    ? SCRIPT_SRC.slice(0, SCRIPT_SRC.lastIndexOf('/') + 1)
    : '';

  const API_BASE = (
    (typeof window !== 'undefined' && window.BLUEPRINTS_API_BASE) || ''
  ).replace(/\/$/, '');

  const SEEDS = (typeof window !== 'undefined' && window.BLUEPRINTS_SEED_NODES) || [];
  let SELECTOR_CFG = {
    enabledButtons: [],
    pages: null,
    showPagingButton: true,
    side: 'right',
    pageSize: 4,
    nodeSwitchPath: '/ui/',
  };

  const BUTTON_DEFS = {
    'fallback-ui': { icon: '🧰', label: 'Fallback UI', buildPath: () => '/fallback-ui/' },
    'ui': { icon: '🏠', label: 'UI', buildPath: () => '/' },
    'database-tables': { icon: '🗂️', label: 'Database Tables', buildPath: () => `${getDbBasePath()}/database-tables.html` },
    'database-diagram': { icon: '🕸️', label: 'Database Diagram', buildPath: () => `${getDbBasePath()}/database-diagram.html` },
  };

  const LS_NODES = 'bp_nodes_v2';
  const LS_CURRENT = 'bp_current_v2';
  const LS_BUTTON_PAGE = 'bp_button_page_v1';

  const HEALTH_INTERVAL = 10_000;
  const DOWN_RETRY = 60_000;
  const LIST_REFRESH = 60_000;
  const LS_TTL = 5 * 60_000;

  let _nodes = [];
  let _current = null;
  let _buttonPage = 0;

  function lsGet(key) { try { return JSON.parse(localStorage.getItem(key)); } catch { return null; } }
  function lsSet(key, val) { try { localStorage.setItem(key, JSON.stringify(val)); } catch {} }

  function loadButtonPage() {
    const page = lsGet(LS_BUTTON_PAGE);
    _buttonPage = Number.isInteger(page) && page >= 0 ? page : 0;
  }

  function saveButtonPage() {
    lsSet(LS_BUTTON_PAGE, _buttonPage);
  }

  function applySelectorConfigFromWindow() {
    const raw = (typeof window !== 'undefined' && window.BLUEPRINTS_SELECTOR_BUTTONS) || {};
    SELECTOR_CFG = {
      enabledButtons: Array.isArray(raw.enabledButtons) ? raw.enabledButtons : [],
      pages: Array.isArray(raw.pages) ? raw.pages : null,
      showPagingButton: raw.showPagingButton !== false,
      side: raw.side === 'left' ? 'left' : 'right',
      pageSize: Number.isInteger(raw.pageSize) && raw.pageSize > 0 ? raw.pageSize : 4,
      nodeSwitchPath: raw.nodeSwitchPath || '/ui/',
    };
  }

  function tryLoadScript(url) {
    return new Promise(resolve => {
      const s = document.createElement('script');
      s.src = url;
      s.async = true;
      s.onload = () => resolve(true);
      s.onerror = () => resolve(false);
      document.head.appendChild(s);
    });
  }

  async function ensureSelectorConfig() {
    if (typeof window === 'undefined' || typeof document === 'undefined') return;
    if (window.BLUEPRINTS_SELECTOR_BUTTONS) return;
    if (window.__bpSelectorConfigPromise) {
      await window.__bpSelectorConfigPromise;
      return;
    }

    const customUrl = window.BLUEPRINTS_SELECTOR_CONFIG_URL;
    const candidates = customUrl
      ? [customUrl]
      : [
          `${window.location.origin}/ui/db/database-pages.config.js`,
          `${window.location.origin}/fallback-ui/db/database-pages.config.js`,
          SCRIPT_DIR ? `${SCRIPT_DIR}blueprints-node-selector.config.js` : '',
        ].filter(Boolean);

    window.__bpSelectorConfigPromise = (async () => {
      for (const url of candidates) {
        const ok = await tryLoadScript(url);
        if (ok && window.BLUEPRINTS_SELECTOR_BUTTONS) return;
      }
    })();

    await window.__bpSelectorConfigPromise;
  }

  async function refreshNodeList() {
    const origin = API_BASE || window.location.origin;

    let selfNode = null;
    try {
      const r = await fetch(`${origin}/health`, { signal: AbortSignal.timeout(5000) });
      if (r.ok) {
        const h = await r.json();
        selfNode = {
          id: h.node_id,
          name: h.node_name || h.node_id,
          uiUrl: (h.ui_url || origin).replace(/\/$/, ''),
          healthUrl: `${origin}/health`,
        };
      }
    } catch {}

    let peers = [];
    try {
      const r = await fetch(`${origin}/api/v1/nodes`, { signal: AbortSignal.timeout(5000) });
      if (r.ok) peers = await r.json();
    } catch {}

    const fresh = [];
    if (selfNode) fresh.push(selfNode);

    for (const p of peers) {
      const syncAddr = (p.addresses && p.addresses[0]) ? p.addresses[0].replace(/\/$/, '') : null;
      if (!syncAddr) continue;
      if (fresh.find(n => n.id === p.node_id)) continue;

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
          } catch {}
        }
        if (!uiUrl) uiUrl = syncAddr;
      }

      fresh.push({
        id: p.node_id,
        name: p.display_name || p.node_id,
        uiUrl,
        healthUrl: `${uiUrl}/health`,
      });
    }

    for (const s of SEEDS) {
      if (!fresh.find(n => n.id === s.id)) {
        const nodeUrl = (s.url || '').replace(/\/$/, '');
        fresh.push({
          id: s.id,
          name: s.name,
          uiUrl: nodeUrl,
          healthUrl: `${nodeUrl}/health`,
        });
      }
    }

    if (!fresh.length) return;

    const byId = Object.fromEntries(_nodes.map(n => [n.id, n]));
    _nodes = fresh.map(n => Object.assign(
      { available: true, lastChecked: 0 },
      n,
      byId[n.id] ? { available: byId[n.id].available, lastChecked: byId[n.id].lastChecked } : {},
    ));

    if (!_current || !_nodes.find(n => n.id === _current)) {
      _current = (selfNode && selfNode.id) || (_nodes[0] && _nodes[0].id) || null;
    }

    lsSet(LS_NODES, { ts: Date.now(), nodes: _nodes });
    lsSet(LS_CURRENT, _current);
    renderBtn();
    renderActionButtons();
    renderPanel();
  }

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
    else { setDot('down'); await electNew(); }
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
        renderActionButtons();
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
            renderActionButtons();
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

  function init() {
    loadButtonPage();

    const cached = lsGet(LS_NODES);
    if (cached && cached.nodes && (Date.now() - cached.ts) < LS_TTL) {
      _nodes = cached.nodes;
      _current = lsGet(LS_CURRENT) || (_nodes[0] && _nodes[0].id) || null;
    } else if (SEEDS.length) {
      _nodes = SEEDS.map(s => {
        const nodeUrl = (s.url || '').replace(/\/$/, '');
        return {
          id: s.id,
          name: s.name,
          uiUrl: nodeUrl,
          healthUrl: `${nodeUrl}/health`,
          available: true,
          lastChecked: 0,
        };
      });
      _current = _nodes[0] && _nodes[0].id;
    }

    renderBtn();
    renderActionButtons();
    renderPanel();

    refreshNodeList();
    checkCurrent().then(() => renderPanel());

    setInterval(() => checkCurrent().then(() => renderPanel()), HEALTH_INTERVAL);
    setInterval(recheckDown, 15_000);
    setInterval(refreshNodeList, LIST_REFRESH);
  }

  class BlueprintsNodeSelector extends HTMLElement {
    connectedCallback() {
      this.innerHTML = `
        <div class="bp-node-selector">
          <div class="bp-ns-actions bp-ns-actions-left" id="bp-ns-actions-left"></div>
          <button class="bp-ns-btn" id="bp-ns-toggle" aria-haspopup="true" aria-expanded="false">
            <span class="bp-ns-dot" id="bp-ns-dot"></span>
            <span class="bp-ns-name" id="bp-ns-name">…</span>
            <span class="bp-ns-caret">▾</span>
          </button>
          <div class="bp-ns-actions bp-ns-actions-right" id="bp-ns-actions-right"></div>
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
      ensureSelectorConfig().finally(() => {
        applySelectorConfigFromWindow();
        init();
      });
    }
  }

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

  function getCurrentNode() {
    return _nodes.find(n => n.id === _current) || null;
  }

  function getNodeSwitchPath() {
    if (SELECTOR_CFG.nodeSwitchPath === 'current') {
      return `${window.location.pathname}${window.location.search}${window.location.hash}`;
    }
    return SELECTOR_CFG.nodeSwitchPath;
  }

  function toAbsoluteUrl(baseUrl, path) {
    const base = String(baseUrl || '').replace(/\/$/, '');
    const rel = String(path || '/').replace(/^\//, '');
    return `${base}/${rel}`;
  }

  function getDbBasePath() {
    const pathname = window.location.pathname || '';
    if (pathname.startsWith('/fallback-ui/')) return '/fallback-ui/db';
    return '/ui/db';
  }

  function navigateToNodePath(path) {
    const node = getCurrentNode();
    const baseUrl = node ? node.uiUrl : window.location.origin;
    window.location.href = toAbsoluteUrl(baseUrl, path);
  }

  function getButtonPages() {
    if (Array.isArray(SELECTOR_CFG.pages) && SELECTOR_CFG.pages.length) {
      const pages = SELECTOR_CFG.pages
        .map(page => Array.isArray(page) ? page.filter(key => BUTTON_DEFS[key]) : [])
        .filter(page => page.length > 0);
      if (!pages.length) return { pages: [], hasPaging: false };
      const hasPaging = SELECTOR_CFG.showPagingButton && pages.length > 1;
      return { pages, hasPaging };
    }

    const enabled = SELECTOR_CFG.enabledButtons || [];
    const actions = enabled.filter(key => key !== 'paging-button' && BUTTON_DEFS[key]);
    if (!actions.length) return { pages: [], hasPaging: false };

    const pages = [];
    for (let i = 0; i < actions.length; i += SELECTOR_CFG.pageSize) {
      pages.push(actions.slice(i, i + SELECTOR_CFG.pageSize));
    }
    const hasPaging = enabled.includes('paging-button') && pages.length > 1 && SELECTOR_CFG.showPagingButton;
    return { pages, hasPaging };
  }

  function renderActionButtons() {
    const left = document.getElementById('bp-ns-actions-left');
    const right = document.getElementById('bp-ns-actions-right');
    if (!left || !right) return;

    left.innerHTML = '';
    right.innerHTML = '';
    left.classList.remove('show');
    right.classList.remove('show');

    const { pages, hasPaging } = getButtonPages();
    if (!pages.length) return;

    const pageCount = pages.length;
    _buttonPage = ((_buttonPage % pageCount) + pageCount) % pageCount;
    saveButtonPage();
    const currentPageButtons = pages[_buttonPage];

    const target = SELECTOR_CFG.side === 'left' ? left : right;
    target.classList.add('show');

    target.innerHTML = currentPageButtons.map(key => {
      const def = BUTTON_DEFS[key];
      return `<button class="bp-ns-action-btn" data-action="${esc(key)}" title="${esc(def.label)}" aria-label="${esc(def.label)}">${esc(def.icon)}</button>`;
    }).join('');

    if (hasPaging) {
      const nextBtn = document.createElement('button');
      nextBtn.className = 'bp-ns-action-btn';
      nextBtn.dataset.action = 'paging-button';
      nextBtn.title = 'Next Buttons';
      nextBtn.setAttribute('aria-label', 'Next Buttons');
      nextBtn.textContent = '⟳';
      target.appendChild(nextBtn);
    }

    target.querySelectorAll('.bp-ns-action-btn').forEach(btn => {
      btn.addEventListener('click', e => {
        e.preventDefault();
        e.stopPropagation();
        const action = btn.dataset.action;
        if (action === 'paging-button') {
          _buttonPage = (_buttonPage + 1) % pageCount;
          renderActionButtons();
          return;
        }
        const def = BUTTON_DEFS[action];
        if (!def) return;
        navigateToNodePath(def.buildPath());
      });
    });
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
        renderActionButtons();
        renderPanel();
        window.location.href = toAbsoluteUrl(el.dataset.url, getNodeSwitchPath());
      });
    });
  }

  function togglePanel() {
    const p = document.getElementById('bp-ns-panel');
    if (!p) return;
    const open = p.classList.toggle('open');
    const btn = document.getElementById('bp-ns-toggle');
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

  if (!customElements.get('blueprints-node-selector')) {
    customElements.define('blueprints-node-selector', BlueprintsNodeSelector);
  }
})();
