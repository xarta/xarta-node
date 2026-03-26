/* ── Group + tab switching ───────────────────────────────────────────── */
function switchGroup(group) {
  _activeGroup = group;
  document.querySelectorAll('.group-tab').forEach(b =>
    b.classList.toggle('active', b.getAttribute('onclick').includes(`'${group}'`)));
  document.querySelectorAll('.table-nav button[data-group]').forEach(b => {
    b.style.display = b.dataset.group === group ? '' : 'none';
  });
  // Show/hide synthesis split-dropdown menu
  const synthesisWrapper = document.getElementById('synthesisMenuWrapper');
  if (synthesisWrapper) synthesisWrapper.style.display = group === 'synthesis' ? '' : 'none';
  if (group === 'synthesis') {
    SynthesisMenuConfig.showGroup();
    switchTab('manual-links');
    manualLinksShowView(_manualLinksView);
    SynthesisMenuConfig.updateActiveTab('manual-links-' + _manualLinksView);
    return;
  }
  // Show/hide probes split-dropdown menu
  const probesWrapper = document.getElementById('probesMenuWrapper');
  if (probesWrapper) probesWrapper.style.display = group === 'probes' ? '' : 'none';
  if (group === 'probes') {
    ProbesMenuConfig.showGroup();
    switchTab('pfsense-dns');
    ProbesMenuConfig.updateActiveTab('pfsense-dns');
    return;
  }
  // Show/hide settings split-dropdown menu
  const settingsWrapper = document.getElementById('settingsMenuWrapper');
  if (settingsWrapper) settingsWrapper.style.display = group === 'settings' ? '' : 'none';
  if (group === 'settings') {
    SettingsMenuConfig.showGroup();
    switchTab('pve-hosts');
    SettingsMenuConfig.updateActiveTab('pve-hosts');
    return;
  }
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
  if (tab === 'services'       && !_services.length)      loadServices();
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
  if (tab === 'manual-links-table')    { switchTab('manual-links'); manualLinksShowView('table');    return; }
  if (tab === 'manual-links-rendered') { switchTab('manual-links'); manualLinksShowView('rendered'); return; }
  if (tab === 'settings'       && !_settings.length)      loadSettings();
  if (tab === 'settings')                                  { initSoundToggle(); initVolumeSlider(); }
  if (tab === 'keys')                                      loadKeys();
  if (tab === 'certs')                                     loadCerts();
  if (tab === 'docs' && !_docsAll.length)                  loadDocs();
  if (tab === 'docs-list') { if (!_docsAll.length) loadDocs(); else _docsRenderList(); }
  if (tab === 'docs-images')                               openDocImagesModal();
  if (tab === 'ai-providers' && !_aiProviders.length)      loadAiProviders();
  if (tab === 'nav-items'    && !_navItems.length)         loadNavItems();
  if (tab === 'form-controls' && !_fcItems.length)         loadFormControls();
  if (tab === 'bookmarks-main'  && !_bookmarks.length)  loadBookmarks();
  if (tab === 'bookmarks-history')                       loadVisits();
  if (tab === 'bookmarks'        && !_bookmarks.length)  loadBookmarks();
  if (tab === 'bookmarks') { switchTab('bookmarks-main'); return; }
  // Notify responsive layout so the correct page-controls group is shown/hidden
  if (typeof ResponsiveLayout !== 'undefined') ResponsiveLayout.updateControlsForTab(tab);
  // self-diag: just show the shell — user clicks Run to trigger tests
  // PCT live status polling — only while nodes tab is open
  if (tab === 'nodes') {
    if (!_pctPollInterval) {
      _pctPollInterval = setInterval(() => { if (_nodes.length) enrichNodePctStatus(); }, 5000);
    }
    loadBackups();
  } else {
    if (_pctPollInterval) { clearInterval(_pctPollInterval); _pctPollInterval = null; }
  }
}

/* ── Bootstrap ────────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  if (!localStorage.getItem(_LS_SECRET_KEY)) { openApiKeyModal(); }
  if (typeof SoundManager !== 'undefined') SoundManager.init();
  if (typeof FormControlManager !== 'undefined') { FormControlManager.init(); FormControlManager.load(); }
  if (typeof HubModal !== 'undefined') HubModal.init();
  const _urlGroup = new URLSearchParams(window.location.search).get('group');
  if (_urlGroup && ['synthesis', 'probes', 'settings'].includes(_urlGroup)) switchGroup(_urlGroup);
  loadFrontendSettings();
  loadHealth();
  loadManualLinks();
  SynthesisMenuConfig.showGroup();
  SynthesisMenuConfig.updateActiveTab('manual-links-' + _manualLinksView);
  loadSyncStatus();
  setInterval(loadHealth, 15_000);
  setInterval(loadSyncStatus, 30_000);
  ResponsiveLayout.init();
  window.addEventListener('beforeunload', () => ResponsiveLayout.destroy());
  _bmInitEmbedPanel();
  // Auto-resume progress display if reindex was running before page load
  _bmPollReindexProgress();
});
