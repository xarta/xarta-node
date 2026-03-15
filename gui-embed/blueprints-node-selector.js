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

  const POLL_INTERVAL = 2_000;
  const REQUEST_TIMEOUT = Math.max(400, POLL_INTERVAL - 250);
  const NO_RESPONSE_WINDOW = 10_000;
  const PRUNE_UNSEEN_MS = 48 * 60 * 60 * 1000;
  const HEART_WINDOW_RATIO = 0.95;
  const HEART_STEPS = 20;
  const LIST_REFRESH = 60_000;
  const LS_TTL = 5 * 60_000;

  let _nodes = [];
  let _current = null;
  let _buttonPage = 0;
  let _polling = false;
  let _lastPollTick = 0;
  let _heartTimer = null;
  let _heartToken = 0;

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
      const _apiFetch = window.apiFetch || fetch;
      const r = await _apiFetch(`${origin}/api/v1/nodes`, { signal: AbortSignal.timeout(5000) });
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

      // Collect LAN/HTTP fallback addresses — any address from p.addresses that differs
      // from the primary uiUrl (e.g. local VLAN HTTP addresses when uiUrl is HTTPS/Tailscale)
      const altAddresses = (p.addresses || [])
        .map(a => a.replace(/\/$/, ''))
        .filter(a => a && a !== uiUrl);

      fresh.push({
        id: p.node_id,
        name: p.display_name || p.node_id,
        uiUrl,
        healthUrl: `${uiUrl}/health`,
        fleetPeer: p.fleet_peer ?? true,
        altAddresses,
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
      {
        latencyMs: null,
        lastSeenAt: 0,
        lastPolledAt: 0,
        discoveredAt: Date.now(),
      },
      n,
      byId[n.id]
        ? {
            latencyMs: byId[n.id].latencyMs,
            lastSeenAt: byId[n.id].lastSeenAt,
            lastPolledAt: byId[n.id].lastPolledAt,
            discoveredAt: byId[n.id].discoveredAt || Date.now(),
            localMode: byId[n.id].localMode,
            activeHealthUrl: byId[n.id].activeHealthUrl,
          }
        : {},
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
    const start = performance.now();
    try {
      const r = await fetch(node.healthUrl, {
        method: 'GET',
        signal: AbortSignal.timeout(REQUEST_TIMEOUT),
      });
      if (r.ok) {
        node.localMode = false;
        node.activeHealthUrl = null;
        return { ok: true, latencyMs: Math.round(performance.now() - start) };
      }
    } catch {}

    // Primary unreachable — try LAN/fallback addresses
    for (const alt of (node.altAddresses || [])) {
      const t = performance.now();
      try {
        const r = await fetch(`${alt}/health`, {
          method: 'GET',
          signal: AbortSignal.timeout(REQUEST_TIMEOUT),
        });
        if (r.ok) {
          node.localMode = true;
          node.activeHealthUrl = `${alt}/health`;
          return { ok: true, latencyMs: Math.round(performance.now() - t) };
        }
      } catch {}
    }

    node.localMode = false;
    node.activeHealthUrl = null;
    return { ok: false, latencyMs: null };
  }

  function pulseHeart() {
    const heart = document.getElementById('bp-ns-heart');
    if (!heart) return;
    if (_heartTimer) {
      clearInterval(_heartTimer);
      _heartTimer = null;
    }
    _heartToken += 1;
    const token = _heartToken;

    const totalMs = Math.max(100, Math.floor(POLL_INTERVAL * HEART_WINDOW_RATIO));
    const stepMs = Math.max(8, Math.floor((totalMs / 2) / HEART_STEPS));
    const totalFrames = HEART_STEPS * 2;
    let frame = 0;

    setHeartIllumination(0);
    heart.classList.remove('stopped');

    _heartTimer = setInterval(() => {
      if (token !== _heartToken) {
        clearInterval(_heartTimer);
        _heartTimer = null;
        return;
      }

      frame += 1;
      let illumination = 0;
      if (frame <= HEART_STEPS) {
        illumination = frame / HEART_STEPS;
      } else {
        illumination = Math.max(0, (totalFrames - frame) / HEART_STEPS);
      }
      setHeartIllumination(illumination);

      if (frame >= totalFrames) {
        clearInterval(_heartTimer);
        _heartTimer = null;
        setHeartIllumination(0);
      }
    }, stepMs);
  }

  function updateHeartDeadman() {
    const heart = document.getElementById('bp-ns-heart');
    if (!heart) return;
    const stalled = !_lastPollTick || (Date.now() - _lastPollTick) > (POLL_INTERVAL * 2.5);
    if (stalled) {
      if (_heartTimer) {
        clearInterval(_heartTimer);
        _heartTimer = null;
      }
      _heartToken += 1;
      setHeartIllumination(0);
      heart.classList.add('stopped');
    } else {
      heart.classList.remove('stopped');
    }
  }

  function setHeartIllumination(value) {
    const heart = document.getElementById('bp-ns-heart');
    if (!heart) return;
    const normalized = Math.min(1, Math.max(0, Number(value) || 0));
    heart.style.setProperty('--bp-heart-illum', normalized.toFixed(3));
  }

  function pickBestCurrent() {
    if (_current && _nodes.find(n => n.id === _current)) return;
    const best = _nodes
      .filter(n => Number.isFinite(n.latencyMs))
      .sort((a, b) => a.latencyMs - b.latencyMs)[0];
    _current = (best && best.id) || (_nodes[0] && _nodes[0].id) || null;
    lsSet(LS_CURRENT, _current);
  }

  function pruneUnseenNodes() {
    const now = Date.now();
    _nodes = _nodes.filter(node => {
      const base = node.lastSeenAt || node.discoveredAt || now;
      return (now - base) <= PRUNE_UNSEEN_MS;
    });
  }

  async function pollNodes() {
    if (_polling || !_nodes.length) return;
    _polling = true;
    _lastPollTick = Date.now();
    pulseHeart();

    await Promise.all(_nodes.map(async node => {
      const result = await pingNode(node);
      const now = Date.now();
      node.lastPolledAt = now;
      if (result.ok) {
        node.latencyMs = result.latencyMs;
        node.lastSeenAt = now;
      } else {
        node.latencyMs = null;
      }
    }));

    pruneUnseenNodes();
    pickBestCurrent();

    updateSelectedDot();

    lsSet(LS_NODES, { ts: Date.now(), nodes: _nodes });
    renderBtn();
    renderActionButtons();
    renderPanel();
    _polling = false;
  }

  function updateSelectedDot() {
    const selected = _nodes.find(n => n.id === _current);
    if (!selected || !Number.isFinite(selected.latencyMs)) {
      setDot('down');
      return;
    }
    if (selected.latencyMs < 30) {
      setDot('ok');
      return;
    }
    if (selected.latencyMs <= 60) {
      setDot('checking');
      return;
    }
    setDot('down');
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

    refreshNodeList().then(() => pollNodes());

    setInterval(pollNodes, POLL_INTERVAL);
    setInterval(updateHeartDeadman, 1_000);
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
            <div class="bp-ns-header">Blueprints nodes <span id="bp-ns-heart" class="bp-ns-heart" aria-hidden="true">❤️</span></div>
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
      list.innerHTML = '<div class="bp-ns-node bp-ns-node-empty">Discovering nodes…</div>';
      return;
    }

    const now = Date.now();
    list.innerHTML = _nodes.map(n => `
      <div class="bp-ns-node${n.id === _current ? ' active' : ''}"
           data-id="${esc(n.id)}" data-url="${esc(n.uiUrl)}">
        <span class="bp-ns-node-name${n.localMode ? ' bp-ns-node-local' : ''}"${n.fleetPeer === false ? ' style="text-decoration:line-through;opacity:0.55"' : ''}>${esc(n.name)}</span>
        <span class="bp-ns-node-metric ${esc(metricClass(n, now))}${n.localMode ? ' bp-ns-node-local' : ''}"${n.localMode ? ' title="via LAN"' : ''}>${esc(metricText(n, now))}</span>
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

  function metricClass(node, now) {
    if (Number.isFinite(node.latencyMs)) {
      if (node.latencyMs < 30) return 'ms-good';
      if (node.latencyMs <= 60) return 'ms-warn';
      return 'ms-bad';
    }
    if (node.lastSeenAt && (now - node.lastSeenAt) > NO_RESPONSE_WINDOW) return 'ms-stale';
    return 'ms-pending';
  }

  function metricText(node, now) {
    if (Number.isFinite(node.latencyMs)) return `${node.latencyMs} ms`;
    if (node.lastSeenAt && (now - node.lastSeenAt) > NO_RESPONSE_WINDOW) {
      return `last seen ${formatAge(now - node.lastSeenAt)}`;
    }
    if (node.lastSeenAt) return `last seen ${formatAge(now - node.lastSeenAt)}`;
    return 'no response';
  }

  function formatAge(ageMs) {
    const totalSeconds = Math.max(0, Math.floor(ageMs / 1000));
    if (totalSeconds < 60) return `${totalSeconds}s`;
    const totalMinutes = Math.floor(totalSeconds / 60);
    if (totalMinutes < 60) return `${totalMinutes}m`;
    const totalHours = Math.floor(totalMinutes / 60);
    if (totalHours < 24) return `${totalHours}h`;
    const totalDays = Math.floor(totalHours / 24);
    return `${totalDays}d`;
  }

  function esc(s) {
    return String(s).replace(/[&<>"']/g,
      c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  if (!customElements.get('blueprints-node-selector')) {
    customElements.define('blueprints-node-selector', BlueprintsNodeSelector);
  }
})();
