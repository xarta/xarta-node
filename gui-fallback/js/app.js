/* ── Group + tab switching ───────────────────────────────────────────── */
function switchGroup(group) {
  _activeGroup = group;
  document.querySelectorAll('.group-tab').forEach(b =>
    b.classList.toggle('active', b.getAttribute('onclick').includes(`'${group}'`)));
  document.querySelectorAll('.table-nav button[data-group]').forEach(b => {
    b.style.display = b.dataset.group === group ? '' : 'none';
  });
  const firstBtn = document.querySelector(`.table-nav button[data-group="${group}"]`);
  if (firstBtn) {
    const m = firstBtn.getAttribute('onclick').match(/switchTab\('([^']+)'\)/);
    if (m) switchTab(m[1]);
  }
}

function switchTab(tab) {
  document.querySelectorAll('.table-nav button').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  const btn = document.querySelector(`.table-nav button[onclick*="'${tab}'"]`);
  if (btn) btn.classList.add('active');
  const panel = document.getElementById(`tab-${tab}`);
  if (panel) panel.classList.add('active');
  // Lazy-load data on first view
  if (tab === 'machines'       && !_machines.length)      loadMachines();
  if (tab === 'nodes'          && !_nodes.length)         loadNodes();
  if (tab === 'pfsense-dns'    && !_pfsenseDns.length)    loadPfSenseDns();
  if (tab === 'proxmox-config' && !_proxmoxConfig.length) loadProxmoxConfig();
  if (tab === 'dockge-stacks'  && !_dockgeStacks.length)  loadDockgeStacks();
  if (tab === 'caddy-configs'  && !_caddyConfigs.length)  loadCaddyConfigs();
  if (tab === 'pve-hosts'      && !_pveHosts.length)      loadPveHosts();
  if (tab === 'vlans'          && !_vlans.length)         loadVlans();
  if (tab === 'arp-manual'     && !_arpManual.length)     loadArpManual();
  if (tab === 'ssh-targets'    && !_sshTargets.length)    loadSshTargets();
  if (tab === 'manual-links'   && !_manualLinks.length)   loadManualLinks();
  if (tab === 'settings'       && !_settings.length)      loadSettings();
  if (tab === 'keys')                                      loadKeys();
  if (tab === 'certs')                                     loadCerts();
  if (tab === 'docs' && !_docsAll.length)                  loadDocs();
  if (tab === 'ai-providers' && !_aiProviders.length)      loadAiProviders();
  if (tab === 'bookmarks'    && !_bookmarks.length)         loadBookmarks();
  // self-diag: just show the shell — user clicks Run to trigger tests
  // PCT live status polling — only while nodes tab is open
  if (tab === 'nodes') {
    if (!_pctPollInterval) {
      _pctPollInterval = setInterval(() => { if (_nodes.length) enrichNodePctStatus(); }, 5000);
    }
  } else {
    if (_pctPollInterval) { clearInterval(_pctPollInterval); _pctPollInterval = null; }
  }
}

/* ── Bootstrap ────────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  if (!localStorage.getItem(_LS_SECRET_KEY)) { openApiKeyModal(); }
  const _urlGroup = new URLSearchParams(window.location.search).get('group');
  if (_urlGroup && ['synthesis', 'probes', 'settings'].includes(_urlGroup)) switchGroup(_urlGroup);
  loadHealth();
  loadManualLinks();
  loadSyncStatus();
  loadBackups();
  setInterval(loadHealth, 15_000);
  setInterval(loadSyncStatus, 30_000);
});
