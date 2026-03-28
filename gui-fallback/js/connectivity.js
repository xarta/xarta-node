/* ── Connectivity Diagnostic ──────────────────────────────────────────── */
const _LS_DIAG_NODE    = 'bp_diag_node_name';
const _LS_DIAG_HOST    = 'bp_diag_host_parent';
const _LS_DIAG_HOST_TS = 'bp_diag_host_parent_ts';
const _LS_DIAG_NODES   = 'bp_diag_nodes';
const _HOST_TTL_MS     = 3_600_000; // 1 hour

let _diagRunning = false;

function _saveDiagNodes(nodes) {
  try {
    // addresses = raw port-8080 sync addresses (backend use only)
    // ui_url/primary_ip = Caddy HTTPS target for browser health checks
    const slim = nodes.map(n => ({
      node_id: n.node_id,
      display_name: n.display_name,
      addresses: n.addresses,
      tailnet: n.tailnet,
      ui_url: n.ui_url,
      primary_ip: n.primary_ip,
    }));
    localStorage.setItem(_LS_DIAG_NODES, JSON.stringify(slim));
  } catch (_) {}
}

// Like apiFetch but does NOT trigger openApiKeyModal on 401 (for cross-node diagnostic calls)
async function _diagFetch(url, options = {}) {
  const secret = localStorage.getItem(_LS_SECRET_KEY) || '';
  const token  = await _computeApiToken(secret);
  const headers = { ...(options.headers || {}), ...(token ? { 'X-API-Token': token } : {}) };
  return fetch(url, { ...options, headers });
}

async function _checkPeerNodes(peers, thisNodeId) {
  const others = peers.filter(n => n.node_id !== thisNodeId && (n.ui_url || n.primary_ip));
  return Promise.all(others.map(async n => {
    // Browser checks must never use raw mTLS addresses (e.g. :8443) because
    // the browser does not present a client cert for those requests.
    const addr = n.ui_url
      ? n.ui_url.replace(/\/$/, '')
      : (n.primary_ip ? `https://${n.primary_ip}` : '');
    if (!addr) {
      return {
        node_id: n.node_id,
        display_name: n.display_name || n.node_id,
        address: '(no ui_url/primary_ip)',
        reachable: false,
        tailnet: n.tailnet,
      };
    }
    try {
      const r = await _diagFetch(`${addr}/health`, { signal: AbortSignal.timeout(4000) });
      return { node_id: n.node_id, display_name: n.display_name || n.node_id, address: addr, reachable: r.ok, tailnet: n.tailnet };
    } catch {
      return { node_id: n.node_id, display_name: n.display_name || n.node_id, address: addr, reachable: false, tailnet: n.tailnet };
    }
  }));
}

async function _checkInternet() {
  const targets = [
    { label: '8.8.8.8',   url: 'https://dns.google/',      desc: 'Google DNS'  },
    { label: '8.8.4.4',   url: 'https://8.8.4.4/',         desc: 'Google (secondary)' },
    { label: '1.1.1.1',   url: 'https://one.one.one.one/', desc: 'Cloudflare DNS'  },
  ];
  return Promise.all(targets.map(async t => {
    try {
      const ac  = new AbortController();
      const tid = setTimeout(() => ac.abort(), 4000);
      await fetch(t.url, { mode: 'no-cors', signal: ac.signal });
      clearTimeout(tid);
      return { ...t, reachable: true };
    } catch {
      return { ...t, reachable: false };
    }
  }));
}

async function _getPctStatusViaPeer(peerAddr, nodeId) {
  try {
    const url = `${peerAddr}/api/v1/nodes/${encodeURIComponent(nodeId)}/pct-status`;
    const r = await _diagFetch(url, { signal: AbortSignal.timeout(8000) });
    if (!r.ok) return { error: `HTTP ${r.status}` };
    return await r.json();
  } catch (e) {
    return { error: e.message };
  }
}

async function _checkPveHostReachability(peerAddr, pveId) {
  try {
    const url = `${peerAddr}/api/v1/pve-hosts/${encodeURIComponent(pveId)}/reachable`;
    const r = await _diagFetch(url, { signal: AbortSignal.timeout(12000) });
    if (!r.ok) return { error: `HTTP ${r.status}` };
    return await r.json();
  } catch (e) {
    return { error: e.message };
  }
}

async function _probePctStatusViaPeers(reachablePeers, nodeId) {
  const errors = [];
  for (const peer of reachablePeers) {
    const result = await _getPctStatusViaPeer(peer.address, nodeId);
    if (result && !result.error) return { peer, result, errors };
    errors.push({ peer, error: (result && result.error) || 'Unknown error' });
  }
  return { peer: null, result: errors.length ? { error: errors[0].error } : null, errors };
}

async function _probeHostReachabilityViaPeers(reachablePeers, pveId) {
  const errors = [];
  for (const peer of reachablePeers) {
    const result = await _checkPveHostReachability(peer.address, pveId);
    if (result && !result.error) return { peer, result, errors };
    errors.push({ peer, error: (result && result.error) || 'Unknown error' });
  }
  return { peer: null, result: errors.length ? { error: errors[0].error } : null, errors };
}

function _diagPeerErrorSummary(errors) {
  if (!errors || !errors.length) return '';
  return errors
    .slice(0, 2)
    .map(entry => `${entry.peer.display_name}: ${entry.error}`)
    .join('; ');
}

function _openDiagModal(modal) {
  if (typeof HubModal !== 'undefined') {
    HubModal.open(modal);
    return;
  }
  if (!modal.open) modal.showModal();
}

function _setDiagStatus(message, tone = 'muted') {
  const el = document.getElementById('diag-modal-status');
  if (!el) return;
  if (!message) {
    el.textContent = '';
    el.style.display = 'none';
    el.style.color = 'var(--text-dim)';
    return;
  }
  el.textContent = message;
  el.style.display = 'block';
  el.style.color = tone === 'warn' ? 'var(--warn)' : 'var(--text-dim)';
}

function _setDiagBusy(isBusy) {
  const retryBtn = document.getElementById('diag-retry-btn');
  const apiBtn = document.getElementById('diag-api-key-btn');
  if (retryBtn) {
    retryBtn.disabled = isBusy;
    retryBtn.textContent = isBusy ? 'Refreshing…' : '↻ Retry';
  }
  if (apiBtn) apiBtn.disabled = isBusy;
}

function _wireDiagModal() {
  const modal = document.getElementById('diag-modal');
  if (!modal || modal.dataset.wired === '1') return;
  const apiBtn = document.getElementById('diag-api-key-btn');
  const retryBtn = document.getElementById('diag-retry-btn');
  if (apiBtn) apiBtn.addEventListener('click', () => openApiKeyModal());
  if (retryBtn) {
    retryBtn.addEventListener('click', () => {
      _diagRunning = false;
      showConnectivityDiagnostic();
    });
  }
  modal.dataset.wired = '1';
}

function _diagRow(icon, label, detail) {
  const detailHtml = detail
    ? `<span style="margin-left:8px;color:var(--text-dim);font-size:12px">${detail}</span>` : '';
  return `<div style="display:flex;align-items:center;gap:6px;padding:5px 0;
      border-bottom:1px solid var(--border)">
    <span style="width:20px;text-align:center;flex-shrink:0;font-size:14px">${icon}</span>
    <span style="flex:1;font-size:13px;font-family:monospace">${label}${detailHtml}</span>
  </div>`;
}

function _diagSection(title) {
  return `<div style="font-size:10px;text-transform:uppercase;letter-spacing:.6px;
      color:var(--text-dim);margin:14px 0 4px;padding-bottom:2px;
      border-bottom:1px solid var(--border)">${title}</div>`;
}

async function showConnectivityDiagnostic() {
  if (_diagRunning) return;
  _diagRunning = true;
  _wireDiagModal();
  _setDiagBusy(true);

  const nodeName = _nodeName
    || localStorage.getItem(_LS_DIAG_NODE)
    || window.location.hostname;

  const hostParent = (() => {
    const el  = document.getElementById('header-host');
    const txt = el ? el.textContent.replace(/[^\w\-\.]/g, '').trim() : '';
    if (txt && txt !== '\u2014') return txt;
    return localStorage.getItem(_LS_DIAG_HOST) || 'Unknown';
  })();

  const cachedNodes = (() => {
    try { return JSON.parse(localStorage.getItem(_LS_DIAG_NODES) || '[]'); } catch { return []; }
  })();

  const cachedPveHosts = (() => {
    try { return JSON.parse(localStorage.getItem(_LS_PVE_HOSTS) || '[]'); } catch { return []; }
  })();

  const modal = document.getElementById('diag-modal');
  const body  = document.getElementById('diag-modal-body');
  const hadExistingResults = !!body.dataset.hasResults;
  document.getElementById('diag-node-name').textContent = nodeName;
  document.getElementById('diag-host-name').textContent =
    hostParent !== 'Unknown' ? `on ${hostParent}` : '(host unknown)';

  if (!modal.open) {
    body.innerHTML = '<div style="color:var(--text-dim);font-size:13px;padding:8px 0">⏳ Running checks\u2026</div>';
    delete body.dataset.hasResults;
    _setDiagStatus('Running checks…');
    _openDiagModal(modal);
  } else {
    _setDiagStatus(hadExistingResults
      ? 'Refreshing… showing previous results until new data arrives.'
      : 'Refreshing…');
  }

  try {
    const [peerResults, netResults] = await Promise.all([
      _checkPeerNodes(cachedNodes, nodeName),
      _checkInternet(),
    ]);

    const reachablePeers = peerResults.filter(p => p.reachable);
    const respondingPeer = reachablePeers[0] || null;
    let pctResult = null;
    let pctPeer = null;
    let pctErrors = [];
    if (reachablePeers.length) {
      const pctProbe = await _probePctStatusViaPeers(reachablePeers, nodeName);
      pctResult = pctProbe.result;
      pctPeer = pctProbe.peer;
      pctErrors = pctProbe.errors;
    }

    // --- PVE host reachability (via responding peer) ---
    const matchedPveHost = cachedPveHosts.find(h =>
      (h.pve_name || '').toLowerCase() === hostParent.toLowerCase() ||
      (h.pve_id   || '') === hostParent
    );
    let hostReachResult = null;
    let hostReachPeer = null;
    let hostReachErrors = [];
    if (reachablePeers.length && matchedPveHost) {
      const hostProbe = await _probeHostReachabilityViaPeers(reachablePeers, matchedPveHost.pve_id);
      hostReachResult = hostProbe.result;
      hostReachPeer = hostProbe.peer;
      hostReachErrors = hostProbe.errors;
    }

    let html = '';

    // --- Fleet peers ---
    const otherPeers = cachedNodes.filter(n => n.node_id !== nodeName);
    if (otherPeers.length) {
      html += _diagSection('Fleet nodes');
      if (!peerResults.length) {
        html += _diagRow('\u2014', 'No peer addresses known', 'load Nodes tab while online');
      } else {
        for (const p of peerResults) {
          html += _diagRow(p.reachable ? '\u2705' : '\u274c', esc(p.display_name),
            p.reachable ? 'reachable' : 'unreachable');
        }
      }
    }

    // --- Container status ---
    if (pctResult && !pctResult.error) {
      const st     = (pctResult.status || 'unknown').toLowerCase();
      const icon   = st === 'running' ? '\uD83D\uDFE1' : st === 'stopped' ? '\uD83D\uDD34' : '\u26AA';
      const detail = pctResult.vmid
        ? `VMID ${pctResult.vmid} on ${pctResult.pve_host || hostParent}`
        : (pctResult.pve_host || hostParent || '');
      html += _diagSection(`Container status (via ${esc(pctPeer.display_name)})`);
      html += _diagRow(icon, `${esc(nodeName)} \u2014 ${st}`, detail);
    } else if (reachablePeers.length && pctResult && pctResult.error) {
      const summary = _diagPeerErrorSummary(pctErrors);
      html += _diagSection('Container status');
      html += _diagRow('\u26A0', `${esc(nodeName)} \u2014 check failed`, esc(summary || pctResult.error));
    } else if (peerResults.length && !respondingPeer) {
      html += _diagSection('Container status');
      html += _diagRow('\u2014', 'Cannot check \u2014 no reachable peers', '');
    }

    // --- Host connectivity ---
    if (hostReachResult && !hostReachResult.error) {
      const peerName = hostReachPeer ? hostReachPeer.display_name : 'peer';
      html += _diagSection(`Host connectivity — ${esc(matchedPveHost.pve_name || hostParent)} (via ${esc(peerName)})`);
      const mgmtIcon = hostReachResult.mgmt_reachable ? '\u2705' : '\u274c';
      html += _diagRow(mgmtIcon, `${esc(hostReachResult.mgmt_ip || hostParent)}:${hostReachResult.port || 8006}`,
        hostReachResult.mgmt_reachable ? 'management IP reachable' : 'management IP unreachable');
      if (hostReachResult.tailnet_ip) {
        const tnIcon = hostReachResult.tailnet_reachable ? '\u2705' : '\u274c';
        html += _diagRow(tnIcon, `${esc(hostReachResult.tailnet_ip)}:${hostReachResult.port || 8006}`,
          hostReachResult.tailnet_reachable ? 'tailnet IP reachable' : 'tailnet IP unreachable');
      }
    } else if (reachablePeers.length && matchedPveHost && hostReachResult && hostReachResult.error) {
      html += _diagSection(`Host connectivity — ${esc(matchedPveHost.pve_name || hostParent)}`);
      html += _diagRow('\u26A0', 'Reachability check failed', esc(_diagPeerErrorSummary(hostReachErrors) || hostReachResult.error));
    } else if (respondingPeer && !matchedPveHost && hostParent !== 'Unknown') {
      html += _diagSection(`Host connectivity — ${esc(hostParent)}`);
      html += _diagRow('\u2014', `${esc(hostParent)} not in PVE hosts cache`,
        'scan Proxmox Hosts tab while online to populate');
    }

    // --- Internet ---
    html += _diagSection('Internet connectivity');
    for (const t of netResults) {
      html += _diagRow(t.reachable ? '\u2705' : '\u274c', t.label, t.desc);
    }

    // --- Diagnosis ---
    const anyInternet  = netResults.some(t => t.reachable);
    const anyPeer      = peerResults.some(p => p.reachable);
    const pctStatus    = pctResult && !pctResult.error
      ? (pctResult.status || 'unknown').toLowerCase() : null;
    const pveHost      = (pctResult && pctResult.pve_host) || hostParent;
    const hostReachable = hostReachResult && !hostReachResult.error
      && (hostReachResult.mgmt_reachable || hostReachResult.tailnet_reachable);
    const hostOffline  = hostReachResult && !hostReachResult.error
      && !hostReachResult.mgmt_reachable && !hostReachResult.tailnet_reachable;

    let diagnosis = '';
    let diagBg    = 'rgba(248,113,113,0.08)';
    let diagBdr   = 'rgba(248,113,113,0.3)';
    if (pctStatus === 'stopped') {
      const peerName = respondingPeer ? respondingPeer.display_name : 'another node';
      diagnosis = `<strong>${esc(nodeName)}</strong> is stopped on <strong>${esc(pveHost)}</strong>.
        Open the Nodes tab on <strong>${esc(peerName)}</strong> to start it.`;
    } else if (pctStatus === 'running') {
      diagBg  = 'rgba(214,158,46,0.08)';
      diagBdr = 'rgba(214,158,46,0.35)';
      diagnosis = `<strong>${esc(nodeName)}</strong> is running but the app is not responding
        \u2014 it may still be starting up, or the service has crashed.
        Check again in a moment, or restart via the Nodes tab on another node.`;
    } else if (hostOffline && anyPeer) {
      diagnosis = `<strong>${esc(pveHost)}</strong> is not reachable on its management or tailnet IP.
        The Proxmox host hosting <strong>${esc(nodeName)}</strong> appears to be offline or unreachable.`;
    } else if (pctResult && pctResult.error && hostReachable && anyPeer) {
      diagBg  = 'rgba(214,158,46,0.08)';
      diagBdr = 'rgba(214,158,46,0.35)';
      diagnosis = `<strong>${esc(nodeName)}</strong> is unreachable, but <strong>${esc(pveHost)}</strong>
        is still reachable. No peer could confirm the container state${pctResult.error ? ` (${esc(pctResult.error)})` : ''}
        \u2014 this can happen during backup, restart, or when peer metadata is stale. Retry shortly or check from another node.`;
    } else if (pctResult && pctResult.error && anyPeer) {
      diagnosis = `<strong>${esc(nodeName)}</strong> is unreachable and other nodes are available,
        but the container state could not be confirmed. Host status is not proven from the current checks.`;
    } else if (anyPeer && !pctResult) {
      diagnosis = `<strong>${esc(nodeName)}</strong> is unreachable and other nodes are available,
        but no peer could confirm the container state yet.`;
    } else if (!anyPeer && anyInternet) {
      diagnosis = `Cannot reach any fleet nodes. Internet is reachable
        \u2014 this may be a fleet-specific or tailnet connectivity issue.
        <br><small style="color:var(--text-dim)">If you are using a Tailscale exit node, try disabling it
        \u2014 it may be routing fleet traffic incorrectly.</small>`;
    } else if (!anyPeer && !anyInternet) {
      diagnosis = `No network connectivity detected. Check your local network connection.
        <br><small style="color:var(--text-dim)">If you are using a Tailscale exit node, try disabling it
        and retrying.</small>`;
    } else {
      diagBg  = 'rgba(100,100,120,0.1)';
      diagBdr = 'rgba(100,100,120,0.3)';
      diagnosis = `<strong>${esc(nodeName)}</strong> is not responding. Unable to determine the cause.`;
    }

    html += `<div style="margin-top:14px;padding:12px 14px;border-radius:var(--radius);
        background:${diagBg};border:1px solid ${diagBdr}">
      <div style="font-size:10px;text-transform:uppercase;letter-spacing:.6px;
          color:var(--text-dim);margin-bottom:6px">Diagnosis</div>
      <div style="font-size:13px;line-height:1.65;color:var(--text)">${diagnosis}</div>
    </div>`;

    body.innerHTML = html;
    body.dataset.hasResults = '1';
    _setDiagStatus('');
  } catch (e) {
    if (!hadExistingResults) {
      body.innerHTML = `<div style="color:var(--err);font-size:13px;padding:8px 0">Could not refresh diagnostics: ${esc(e.message || 'Unknown error')}</div>`;
    }
    _setDiagStatus('Refresh failed. Showing previous results.', 'warn');
  } finally {
    _setDiagBusy(false);
    _diagRunning = false;
  }
}

/* ── State ────────────────────────────────────────────────────────────── */
