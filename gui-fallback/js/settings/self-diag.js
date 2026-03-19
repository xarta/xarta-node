/* ── Self Diagnostic ─────────────────────────────────────────────────── */
const _DIAG_ENDPOINTS = [
  { path: '/health',                              label: 'Health check',              group: 'Core' },
  { path: '/api/v1/schema',                       label: 'Database schema',           group: 'Core' },
  { path: '/api/v1/assumptions',                  label: 'Assumptions',               group: 'Core' },
  { path: '/api/v1/settings',                     label: 'Settings',                  group: 'Core' },
  { path: '/api/v1/nodes',                        label: 'Fleet nodes',               group: 'Nodes' },
  { path: '/api/v1/nodes/self',                   label: 'Self identity',             group: 'Nodes' },
  { path: '/api/v1/sync/status',                  label: 'Sync status',               group: 'Sync' },
  { path: '/api/v1/sync/export',                  label: 'Sync export',               group: 'Sync' },
  { path: '/api/v1/sync/gui/export',              label: 'GUI sync export',           group: 'Sync' },
  { path: '/api/v1/backup',                       label: 'Backup list',               group: 'Backup' },
  { path: '/api/v1/services',                     label: 'Services',                  group: 'Data' },
  { path: '/api/v1/machines',                     label: 'Machines',                  group: 'Data' },
  { path: '/api/v1/arp-manual',                   label: 'Manual ARP',                group: 'Data' },
  { path: '/api/v1/vlans',                        label: 'VLANs',                     group: 'Data' },
  { path: '/api/v1/manual-links',                 label: 'Manual links',              group: 'Data' },
  { path: '/api/v1/pve-hosts',                    label: 'PVE hosts',                 group: 'Proxmox' },
  { path: '/api/v1/pve-hosts/scan/status',        label: 'PVE scan readiness',        group: 'Proxmox' },
  { path: '/api/v1/proxmox-config',               label: 'Proxmox config',            group: 'Proxmox' },
  { path: '/api/v1/proxmox-config/probe/status',  label: 'Proxmox probe status',      group: 'Proxmox' },
  { path: '/api/v1/proxmox-nets',                 label: 'Proxmox nets',              group: 'Proxmox' },
  { path: '/api/v1/pfsense-dns',                  label: 'pfSense DNS',               group: 'pfSense' },
  { path: '/api/v1/pfsense-dns/probe/status',     label: 'pfSense probe status',      group: 'pfSense' },
  { path: '/api/v1/keys/status',                  label: 'SSH key status',            group: 'Keys' },
  { path: '/api/v1/keys/store',                   label: 'Key store list',            group: 'Keys' },
  { path: '/api/v1/ssh-targets',                  label: 'SSH targets',               group: 'Keys' },
  { path: '/api/v1/caddy-configs',                label: 'Caddy configs',             group: 'Services' },
  { path: '/api/v1/caddy-configs/probe/status',   label: 'Caddy probe status',        group: 'Services' },
  { path: '/api/v1/dockge-stacks',                label: 'Dockge stacks',             group: 'Services' },
  { path: '/api/v1/dockge-stacks/probe/status',   label: 'Dockge probe status',       group: 'Services' },
  { path: '/api/v1/dockge-stacks/services',       label: 'Dockge stack services',     group: 'Services' },
  { path: '/api/v1/certs/status',                 label: 'Cert status',               group: 'Services' },
  { path: '/api/v1/docs',                         label: 'Docs',                      group: 'Docs' },
  { path: '/api/v1/doc-groups',                   label: 'Doc groups',                group: 'Docs' },
  { path: '/api/v1/doc-images',                   label: 'Doc images',                group: 'Docs' },
  { path: '/api/v1/todo',                         label: 'Todo list',                 group: 'Data' },
];

async function runSelfDiag() {
  const btn     = document.getElementById('self-diag-run-btn');
  const results = document.getElementById('self-diag-results');
  const status  = document.getElementById('self-diag-status');
  btn.disabled  = true;
  btn.textContent = '\u29d0 Running\u2026';
  status.textContent = 'Running diagnostics\u2026';
  status.style.color = 'var(--text-dim)';
  status.hidden = false;
  results.innerHTML = '';

  const cachedNodes = (() => {
    try { return JSON.parse(localStorage.getItem(_LS_DIAG_NODES) || '[]'); } catch { return []; }
  })();
  // Use node_id (not display name) for peer filtering and node lookups
  const selfId = _selfNodeId || localStorage.getItem(_LS_DIAG_NODE) || '';
  const selfNode = cachedNodes.find(n => n.node_id === selfId);
  const selfTailnet = selfNode?.tailnet || null;

  // Run connectivity + all endpoint tests concurrently
  const [endpointResults, peerResults, netResults, openapiData, mtlsProbeData, sshProbeData, failoverProbeData] = await Promise.all([
    Promise.all(_DIAG_ENDPOINTS.map(async ep => {
      const start = performance.now();
      try {
        const r = await apiFetch(ep.path, { signal: AbortSignal.timeout(6000) });
        const ms = Math.round(performance.now() - start);
        return { ...ep, status: r.status, ok: r.ok, ms };
      } catch (e) {
        return { ...ep, status: null, ok: false, error: e.message, ms: Math.round(performance.now() - start) };
      }
    })),
    _checkPeerNodes(cachedNodes, selfId),
    _checkInternet(),
    (async () => {
      try {
        const r = await apiFetch('/openapi.json', { signal: AbortSignal.timeout(6000) });
        if (!r.ok) return null;
        return await r.json();
      } catch { return null; }
    })(),
    (async () => {
      try {
        const r = await apiFetch('/api/v1/sync/mtls-probe', { signal: AbortSignal.timeout(30000) });
        return r.ok ? await r.json() : null;
      } catch { return null; }
    })(),
    (async () => {
      try {
        const r = await apiFetch('/api/v1/sync/ssh-probe', { signal: AbortSignal.timeout(30000) });
        return r.ok ? await r.json() : null;
      } catch { return null; }
    })(),
    (async () => {
      try {
        const r = await apiFetch('/api/v1/sync/failover-probe', { signal: AbortSignal.timeout(60000) });
        return r.ok ? await r.json() : null;
      } catch { return null; }
    })(),
  ]);

  const testedPaths = new Set(_DIAG_ENDPOINTS.map(e => e.path));
  // Paths handled specially (fetched outside _DIAG_ENDPOINTS or in their own section)
  ['/api/v1/firewall/status', '/api/v1/sync/mtls-probe', '/api/v1/sync/ssh-probe', '/api/v1/sync/failover-probe'].forEach(p => testedPaths.add(p));
  // GET endpoints from OpenAPI that we don't auto-test (parameterised paths)
  const untestedGets = openapiData
    ? Object.entries(openapiData.paths || {})
        .filter(([path, methods]) => 'get' in methods && !testedPaths.has(path))
        .map(([path]) => path)
        .sort()
    : [];

  let html = '';

  // ── Fleet connectivity ─────────────────────────────────────────────
  if (peerResults.length) {
    html += _diagSection('Fleet Connectivity');
    for (const p of peerResults) {
      // Put address in the wider detail column; status word in the narrow extra column
      html += _selfDiagRow(p.reachable ? '\u2705' : '\u274c',
        esc(p.display_name || p.node_id), esc(p.address), p.reachable ? 'reachable' : 'unreachable');
    }
  } else {
    html += _diagSection('Fleet Connectivity');
    html += _selfDiagRow('\u2014', 'No peer addresses cached', 'open Nodes tab while online to populate', '');
  }

  // ── Sync — mTLS Transport ───────────────────────────────────────────────
  html += _diagSection('Sync \u2014 mTLS Transport (drain)');
  if (!mtlsProbeData) {
    html += _selfDiagRow('\u26a0', '/api/v1/sync/mtls-probe', 'endpoint missing \u2014 update app code on this node', '');
  } else {
    const tlsIcon = mtlsProbeData.tls_configured ? '\u2705' : '\u26a0';
    const tlsLabel = mtlsProbeData.tls_configured ? 'mTLS configured (SYNC_TLS_* set)' : 'TLS not configured \u2014 using plain HTTP';
    html += _selfDiagRow(tlsIcon, 'TLS configuration', tlsLabel, '');
    const _mtlsIcons = { ok: '\u2705', tls_error: '\uD83D\uDD12', http_error: '\u26a0', refused: '\u274c', timeout: '\u274c', error: '\u274c' };
    for (const p of (mtlsProbeData.peers || [])) {
      const icon = _mtlsIcons[p.status] || '\u274c';
      const detail = p.error || (p.http_status != null ? `HTTP ${p.http_status}` : p.status);
      html += _selfDiagRow(icon, esc(p.node_id), p.status, esc(detail));
    }
  }

  // ── Sync — SSH Fleet Connectivity ───────────────────────────────────────
  html += _diagSection('Sync \u2014 SSH Fleet Connectivity (fleet-pull)');
  if (!sshProbeData) {
    html += _selfDiagRow('\u26a0', '/api/v1/sync/ssh-probe', 'endpoint missing \u2014 update app code on this node', '');
  } else if (!sshProbeData.ssh_key_present) {
    html += _selfDiagRow('\u274c', 'XARTA_NODE_SSH_KEY', 'key file not found', esc(sshProbeData.error || ''));
  } else {
    html += _selfDiagRow('\u2705', 'XARTA_NODE_SSH_KEY', 'key file present', '');
    const _sshIcons = { ok: '\u2705', auth_failed: '\uD83D\uDD12', host_key_changed: '\u26a0', refused: '\u274c', timeout: '\u274c', no_route: '\u274c', error: '\u274c' };
    for (const p of (sshProbeData.peers || [])) {
      const icon = _sshIcons[p.status] || '\u274c';
      const detail = esc((p.error || p.status).split('\n')[0].trim());
      html += _selfDiagRow(icon, esc(p.node_id), p.status, detail);
    }
  }

  // ── Sync — Failover Logic ───────────────────────────────────────────
  html += _diagSection('Sync \u2014 Failover Logic (simulated VPS probe)');
  if (!failoverProbeData) {
    html += _selfDiagRow('\u26a0', '/api/v1/sync/failover-probe', 'endpoint missing \u2014 update app code on this node', '');
  } else {
    const overallIcon = failoverProbeData.all_passed ? '\u2705' : '\u274c';
    html += _selfDiagRow(overallIcon, 'Overall result',
      failoverProbeData.all_passed ? 'all peers passed' : 'one or more peers failed',
      esc(failoverProbeData.method || ''));
    for (const p of (failoverProbeData.peers || [])) {
      // Dead URL: ✅ if it correctly refused (expected), ⚠️ if somehow open, ❌ other
      const deadExpected = p.dead_status === 'refused' || p.dead_status === 'timeout';
      const deadIcon = deadExpected ? '\u2705' : (p.dead_status === 'open' ? '\u26a0' : '\u274c');
      const deadDetail = deadExpected
        ? `${p.dead_status} in ${p.dead_ms}ms (expected \u2014 port ${failoverProbeData.dead_port} closed)`
        : esc(p.dead_status + (p.dead_error ? ': ' + p.dead_error : ''));
      html += _selfDiagRow(deadIcon, esc(p.node_id) + ' \u2192 dead URL', esc(p.dead_url || ''), deadDetail);
      // Real URL: ✅ if ok, ❌ otherwise
      const realOk = p.real_status === 'ok';
      const realIcon = realOk ? '\u2705' : '\u274c';
      const realDetail = realOk
        ? `ok in ${p.real_ms}ms`
        : esc(p.real_status + (p.real_error ? ': ' + p.real_error : ''));
      html += _selfDiagRow(realIcon, esc(p.node_id) + ' \u2192 real URL', esc(p.real_url || 'none configured'), realDetail);
    }
  }

  // ── Sync — Data Propagation Round-trip ───────────────────────────────
  html += _diagSection('Sync \u2014 Data Propagation Round-trip');
  html += `<div id="bp-roundtrip-section" style="display:flex;align-items:center;gap:8px;padding:5px 2px">
    <button id="bp-roundtrip-btn"
      style="padding:3px 10px;background:var(--accent-dim);color:var(--text);border:1px solid var(--border);border-radius:4px;cursor:pointer;font-size:12px;flex-shrink:0">
      &#x25b6; Test propagation (~25s)
    </button>
    <span style="font-size:12px;color:var(--text-dim)">Writes a canary row, confirms it arrives on a peer, then deletes it</span>
  </div>`;

  // ── Internet ──────────────────────────────────────────────────────
  html += _diagSection('Internet Connectivity');
  for (const t of netResults) {
    html += _selfDiagRow(t.reachable ? '\u2705' : '\u274c', t.label, t.desc, '');
  }

  // ── API endpoints grouped ─────────────────────────────────────────
  const groups = {};
  for (const r of endpointResults) {
    (groups[r.group] = groups[r.group] || []).push(r);
  }
  for (const [grp, items] of Object.entries(groups)) {
    html += _diagSection(`API \u2014 ${grp}`);
    for (const r of items) {
      const icon   = r.ok ? '\u2705' : (r.status === 401 || r.status === 403 ? '\uD83D\uDD12' : '\u274c');
      const detail = r.error ? `error: ${r.error}` : `HTTP ${r.status}`;
      html += _selfDiagRow(icon, r.path, r.label, r.ok ? `${r.ms}ms` : detail);
    }
  }

  // ── Untested GET endpoints ─────────────────────────────────────────
  if (untestedGets.length) {
    html += _diagSection('GET Endpoints Not Auto-Tested (require path parameters)');
    for (const path of untestedGets) {
      html += _selfDiagRow('\u2014', path, 'needs specific ID \u2014 not auto-testable', '');
    }
  } else if (!openapiData) {
    html += _diagSection('GET Endpoints Coverage');
    html += _selfDiagRow('\u26a0', 'Could not fetch /openapi.json', 'endpoint coverage unknown', '');
  }

  // ── Firewall probe ─────────────────────────────────────────────────
  // Ask a responding peer to probe this node's ports.
  // If no peer is available, fall back to the local /api/v1/firewall/status
  // which reports iptables state (no external vantage point).
  const myAddress = (() => {
    try {
      const self = (JSON.parse(localStorage.getItem(_LS_DIAG_NODES) || '[]'))
        .find(n => n.node_id === selfId);
      return self && self.addresses && self.addresses[0] ? self.addresses[0] : null;
    } catch { return null; }
  })();

  const proberPeer = (() => {
    // Prefer a peer on the same tailnet — tailscale ping only works within
    // a tailnet, so a same-tailnet prober gives a definitive UDP 41641 result.
    const sameTailnetPeer = selfTailnet
      ? peerResults.find(p => p.reachable && p.tailnet === selfTailnet)
      : null;
    return sameTailnetPeer || peerResults.find(p => p.reachable) || null;
  })();
  const proberSameTailnet = !!(proberPeer && selfTailnet && proberPeer.tailnet === selfTailnet);

  html += _diagSection('Firewall — Local iptables Status');
  let localFwOk = false;
  try {
    const fwR = await apiFetch('/api/v1/firewall/status', { signal: AbortSignal.timeout(6000) });
    if (fwR.ok) {
      const fw = await fwR.json();
      const policyOk = fw.input_policy === 'DROP';
      html += _selfDiagRow(policyOk ? '\u2705' : '\u26a0',
        'INPUT default policy', fw.input_policy,
        policyOk ? '' : 'should be DROP');
      html += _selfDiagRow(fw.xarta_input_chain ? '\u2705' : '\u26a0',
        'XARTA_INPUT chain', fw.xarta_input_chain ? 'present' : 'missing',
        fw.xarta_input_chain ? '' : 'run setup-firewall.sh');
      for (const p of (fw.ports || [])) {
        const ok = p.expected === 'open' ? p.in_ruleset : !p.in_ruleset;
        html += _selfDiagRow(ok ? '\u2705' : '\u26a0',
          `TCP/UDP ${p.port} — ${p.label}`,
          p.in_ruleset ? 'in XARTA_INPUT' : 'not in XARTA_INPUT',
          p.expected === 'open' ? 'should be allowed' : '');
      }
      localFwOk = policyOk && fw.xarta_input_chain;
    } else {
      html += _selfDiagRow('\u274c', '/api/v1/firewall/status', `HTTP ${fwR.status}`, '');
    }
  } catch (e) {
    html += _selfDiagRow('\u274c', '/api/v1/firewall/status', e.message, '');
  }

  let probePassCount = 0, probeTotalCount = 0;
  if (proberPeer && myAddress) {
    const tailnetNote = proberSameTailnet ? ' — same tailnet' : ' — cross-tailnet';
    html += _diagSection(`Firewall — External Port Probe (via ${esc(proberPeer.display_name || proberPeer.node_id)}${tailnetNote})`);
    status.textContent = `Probing ports via ${proberPeer.display_name || proberPeer.node_id}\u2026`;
    try {
      const probeR = await _diagFetch(`${proberPeer.address}/api/v1/firewall/probe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target: myAddress, target_node_id: selfId }),
        signal: AbortSignal.timeout(30000),
      });
      if (probeR.ok) {
        const probe = await probeR.json();
        for (const p of (probe.ports || [])) {
          if (p.result === 'skipped') {
            html += _selfDiagRow('\u2014', `${p.proto.toUpperCase()} ${p.port} — ${esc(p.label)}`, 'skipped (probe tool unavailable)', '');
            continue;
          }
          probeTotalCount++;
          const icon = p.pass ? '\u2705' : '\u274c';
          const expectLabel = p.expected === 'open' ? 'should be open' : 'should be blocked';
          html += _selfDiagRow(icon,
            `${p.proto.toUpperCase()} ${p.port} — ${esc(p.label)}`,
            `${expectLabel} \u2192 ${p.result}`,
            p.pass ? '' : '\u26a0 unexpected');
          if (p.pass) probePassCount++;
        }
        if (!probe.all_pass) {
          html += _selfDiagRow('\u26a0', 'Overall probe result', 'FAIL — some ports returned unexpected results', '');
        }
      } else {
        html += _selfDiagRow('\u274c', 'Probe request failed', `HTTP ${probeR.status}`, '');
      }
    } catch (e) {
      html += _selfDiagRow('\u274c', 'Probe request error', e.message, '');
    }
  } else if (!proberPeer) {
    html += _diagSection('Firewall — External Port Probe');
    html += _selfDiagRow('\u2014', 'No reachable peer available', 'external probe requires a responding fleet peer', '');
  } else {
    html += _diagSection('Firewall — External Port Probe');
    html += _selfDiagRow('\u2014', 'Own address not cached', 'open the Nodes tab while online to populate', '');
  }

  results.innerHTML = html;

  // Bind the propagation round-trip test button
  const _rtBtn = document.getElementById('bp-roundtrip-btn');
  const _rtSection = document.getElementById('bp-roundtrip-section');
  if (_rtBtn) {
    _rtBtn.addEventListener('click', async () => {
      _rtBtn.disabled = true;
      _rtBtn.textContent = '\u29d0 Running\u2026';
      try {
        const r = await apiFetch('/api/v1/sync/roundtrip-test', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: AbortSignal.timeout(35000),
        });
        if (r.ok) {
          const d = await r.json();
          const _rtIcons = { ok: '\u2705', timeout: '\u274c', auth_failed: '\uD83D\uDD12', no_peers: '\u2014', no_secret: '\u274c', error: '\u274c' };
          const icon = _rtIcons[d.status] || '\u274c';
          const detail = d.status === 'ok'
            ? `propagated to ${esc(d.propagated_to)} in ${d.elapsed_ms}ms`
            : esc(d.error || d.status);
          _rtSection.outerHTML = _selfDiagRow(icon, 'data propagation round-trip', d.status, detail);
        } else {
          _rtSection.outerHTML = _selfDiagRow('\u274c', 'data propagation round-trip', `HTTP ${r.status}`, '');
        }
      } catch (e) {
        _rtSection.outerHTML = _selfDiagRow('\u274c', 'data propagation round-trip', e.message, '');
      }
    });
  }

  const total   = endpointResults.length;
  const passed  = endpointResults.filter(r => r.ok).length;
  const peersOk = peerResults.filter(p => p.reachable).length;
  const fwSummary = probeTotalCount
    ? ` \u2022 firewall probe ${probePassCount}/${probeTotalCount} pass`
    : '';
  const mtlsOk = (mtlsProbeData?.peers || []).filter(p => p.status === 'ok').length;
  const mtlsTotal = (mtlsProbeData?.peers || []).length;
  const sshOk = (sshProbeData?.peers || []).filter(p => p.status === 'ok').length;
  const sshTotal = (sshProbeData?.peers || []).length;
  const mtlsSummary = mtlsTotal ? ` \u2022 mTLS ${mtlsOk}/${mtlsTotal}` : '';
  const sshSummary  = sshTotal  ? ` \u2022 SSH ${sshOk}/${sshTotal}`   : '';
  status.textContent = `Done \u2014 ${passed}/${total} API endpoints OK \u2022 ${peersOk}/${peerResults.length} peers reachable${mtlsSummary}${sshSummary}${fwSummary}`;
  status.style.color = (passed === total && mtlsOk === mtlsTotal && sshOk === sshTotal && (probeTotalCount === 0 || probePassCount === probeTotalCount)) ? 'var(--accent)' : '#f87171';
  btn.disabled = false;
  btn.textContent = '\u25b6 Run Diagnostics';
}

function _selfDiagRow(icon, label, detail, extra) {
  return `<div style="display:flex;align-items:baseline;gap:8px;padding:4px 2px;border-bottom:1px solid var(--border);font-size:13px">
    <span style="width:22px;flex-shrink:0;text-align:center">${icon}</span>
    <code style="flex:1.4;font-size:12px;word-break:break-all;color:var(--text)">${esc(label)}</code>
    <span style="flex:1.2;color:var(--text-dim);font-size:12px;word-break:break-all">${esc(detail || '')}</span>
    <span style="flex:0.6;color:var(--text-dim);font-size:11px;text-align:right">${esc(extra || '')}</span>
  </div>`;
}
