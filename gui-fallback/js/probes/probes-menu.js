// probes-menu.js — Split-dropdown navigation for the Probes group
// xarta-node Blueprints GUI
//
// Thin wrapper around createHubMenu() (hub-menu.js).
// Contains only the Probes-specific config, defaultMenu, and function registrations.
//
// localStorage key: 'blueprintsProbesMenuConfig'
// No inline event handlers — all event wiring via addEventListener.

'use strict';

const ProbesMenuConfig = createHubMenu({
    storageKey:      'blueprintsProbesMenuConfig',
    group:           'probes',
    toggleId:        'probesMenuToggle',
    tabsId:          'probesHubTabs',
    currentLabelId:  'probesCurrentTabLabel',
    saveButtonId:    'probesMenuSaveButton',
    resetButtonId:   'probesMenuResetButton',
    editorListId:    'probesMenuEditorList',
    notificationId:  'probesMenuSaveNotification',
    resetConfirmMsg: 'Reset probes menu to default layout?',
    // Mobile: the layout/context button is pinned outside the hamburger menu
    mobilePinnedId:  'probes-settings',
    pinnedTabsId:    'probesHubTabsPinned',
    defaultMenu: [
        { id: 'pfsense-dns',          label: 'pfSense DNS',    icon: 'icons/ui/pfsense-blue.svg',  pageLabel: 'pfSense DNS',       parent: null,              order: 0 },
        { id: 'proxmox-config',       label: 'Proxmox Config', icon: 'icons/ui/proxmox-blue.svg',  pageLabel: 'Proxmox Config',    parent: null,              order: 1 },
        { id: 'vlans',                label: 'VLANs',            icon: HIEROGLYPHS.nileWaves,  pageLabel: 'VLANs',             parent: 'proxmox-config',  order: 0 },
        { id: 'ssh-targets',          label: 'SSH Targets',      icon: HIEROGLYPHS.doorBolt,   pageLabel: 'SSH Targets',       parent: 'proxmox-config',  order: 1 },
        { id: 'dockge-stacks',        label: 'Dockge Stacks',    icon: HIEROGLYPHS.pyramid,    pageLabel: 'Dockge Stacks',     parent: 'proxmox-config',  order: 2 },
        { id: 'caddy-configs',        label: 'Caddy Configs',    icon: HIEROGLYPHS.solarBoat,  pageLabel: 'Caddy Configs',     parent: 'proxmox-config',  order: 3 },
        { id: 'bookmarks',            label: 'Bookmarks',        icon: HIEROGLYPHS.papyrus,    pageLabel: 'Bookmarks',         parent: null,              order: 2 },
        { id: 'bookmarks-main',       label: 'Main',             icon: HIEROGLYPHS.papyrus,    pageLabel: 'Bookmarks',         parent: 'bookmarks',       order: 0 },
        { id: 'bookmarks-history',    label: 'History',          icon: HIEROGLYPHS.starDuat,   pageLabel: 'Visit History',     parent: 'bookmarks',       order: 1 },
        { id: 'bookmarks-embeddings', label: 'Embeddings',       icon: HIEROGLYPHS.falcon,     pageLabel: 'Embedding Config',  parent: 'bookmarks',       order: 2 },
        { id: 'bookmarks-setup',      label: 'Setup',            icon: HIEROGLYPHS.djedPillar, pageLabel: 'Setup & Import',    parent: 'bookmarks',       order: 3 },
        { id: 'probes-settings',      label: '☰',                icon: HIEROGLYPHS.kheper,     pageLabel: 'Navbar Layout',     parent: null,              order: 3 },

        // ── Bookmarks page function items ─────────────────────────────────
        { id: 'bm-fn-add',    label: 'Add Bookmark', icon: 'icons/ui/plus-blue.svg',          fn: 'bm.add',         activeOn: ['bookmarks-main'], parent: 'probes-settings', order: 0 },
        { id: 'bm-fn-import', label: 'Import HTML',  icon: HIEROGLYPHS.papyrus,                fn: 'bm.import',      activeOn: ['bookmarks-main'], parent: 'probes-settings', order: 1 },
        { id: 'bm-fn-refresh',label: 'Refresh',      icon: HIEROGLYPHS.nefer,                  fn: 'bm.refresh',     activeOn: ['bookmarks-main'], parent: 'probes-settings', order: 2 },
        { id: 'bm-fn-cols',   label: 'Columns',      icon: 'icons/ui/table-columns-blue.svg',  fn: 'bm.cols',        activeOn: ['bookmarks-main'], parent: 'probes-settings', order: 3 },
        { id: 'bm-fn-expl',   label: 'Explain Sort',    icon: HIEROGLYPHS.eyeOfHorus,  fn: 'bm.explainSort', activeOn: ['bookmarks-main'], parent: 'probes-settings', order: 4 },
        { id: 'bm-fn-dead',   label: 'Dead links',      icon: HIEROGLYPHS.shen,        fn: 'bm.deadLinks',   activeOn: ['bookmarks-main'], parent: 'probes-settings', order: 5 },

        // ── pfSense DNS page function items ───────────────────────────────
        { id: 'dns-fn-refresh',  label: 'Refresh',       icon: HIEROGLYPHS.nefer,      fn: 'dns.refresh',    activeOn: ['pfsense-dns'], parent: 'probes-settings', order: 0 },
        { id: 'dns-fn-probe',    label: 'Probe pfSense', icon: HIEROGLYPHS.wasScepter, fn: 'dns.probe',      activeOn: ['pfsense-dns'], parent: 'probes-settings', order: 1 },
        { id: 'dns-fn-sweep',    label: 'Ping Sweep',    icon: HIEROGLYPHS.wasScepter, fn: 'dns.sweep',      activeOn: ['pfsense-dns'], parent: 'probes-settings', order: 2 },
        { id: 'dns-fn-expand',   label: 'Expand all',   icon: 'icons/ui/chevron-down-blue.svg', fn: 'dns.expandAll',  activeOn: ['pfsense-dns'], parent: 'probes-settings', order: 3 },
        { id: 'dns-fn-collapse', label: 'Collapse all', icon: 'icons/ui/chevron-up-blue.svg',   fn: 'dns.collapseAll',activeOn: ['pfsense-dns'], parent: 'probes-settings', order: 4 },

        // ── Proxmox Config page function items ────────────────────────────
        { id: 'pve-fn-refresh',   label: 'Refresh',     icon: HIEROGLYPHS.nefer,      fn: 'pve.refresh',    activeOn: ['proxmox-config'], parent: 'probes-settings', order: 0 },
        { id: 'pve-fn-fullprobe', label: 'Full Probe',  icon: HIEROGLYPHS.wasScepter, fn: 'pve.fullProbe',  activeOn: ['proxmox-config'], parent: 'probes-settings', order: 1 },
        { id: 'pve-fn-steps',     label: 'Steps',       icon: HIEROGLYPHS.djedPillar, fn: 'pve.steps',      activeOn: ['proxmox-config'], parent: 'probes-settings', order: 2 },
        { id: 'pve-fn-expand',    label: 'Expand all',   icon: 'icons/ui/chevron-down-blue.svg', fn: 'pve.expandAll',  activeOn: ['proxmox-config'], parent: 'probes-settings', order: 3 },
        { id: 'pve-fn-collapse',  label: 'Collapse all', icon: 'icons/ui/chevron-up-blue.svg',   fn: 'pve.collapseAll',activeOn: ['proxmox-config'], parent: 'probes-settings', order: 4 },

        // ── VLANs page function items ──────────────────────────────────────
        { id: 'vlan-fn-refresh',    label: 'Refresh',             icon: HIEROGLYPHS.nefer,      fn: 'vlan.refresh',    activeOn: ['vlans'],             parent: 'probes-settings', order: 0 },

        // ── SSH Targets page function items ────────────────────────────────
        { id: 'ssh-fn-rebuild',     label: 'Rebuild from config', icon: HIEROGLYPHS.doorBolt,   fn: 'ssh.rebuild',     activeOn: ['ssh-targets'],       parent: 'probes-settings', order: 0 },

        // ── Dockge Stacks page function items ──────────────────────────────
        { id: 'dockge-fn-refresh',  label: 'Refresh',             icon: HIEROGLYPHS.nefer,      fn: 'dockge.refresh',  activeOn: ['dockge-stacks'],     parent: 'probes-settings', order: 0 },
        { id: 'dockge-fn-probe',    label: 'Probe Dockge',        icon: HIEROGLYPHS.wasScepter, fn: 'dockge.probe',    activeOn: ['dockge-stacks'],     parent: 'probes-settings', order: 1 },
        { id: 'dockge-fn-expand',   label: 'Expand all',           icon: 'icons/ui/chevron-down-blue.svg', fn: 'dockge.expandAll',activeOn: ['dockge-stacks'],     parent: 'probes-settings', order: 2 },
        { id: 'dockge-fn-collapse', label: 'Collapse all',         icon: 'icons/ui/chevron-up-blue.svg',   fn: 'dockge.collapse', activeOn: ['dockge-stacks'],     parent: 'probes-settings', order: 3 },

        // ── Caddy Configs page function items ──────────────────────────────
        { id: 'caddy-fn-refresh',   label: 'Refresh',             icon: HIEROGLYPHS.nefer,      fn: 'caddy.refresh',   activeOn: ['caddy-configs'],     parent: 'probes-settings', order: 0 },
        { id: 'caddy-fn-probe',     label: 'Probe Caddy',         icon: HIEROGLYPHS.wasScepter, fn: 'caddy.probe',     activeOn: ['caddy-configs'],     parent: 'probes-settings', order: 1 },

        // ── Visit History page function items ──────────────────────────────
        { id: 'vis-fn-refresh',     label: 'Refresh',             icon: HIEROGLYPHS.nefer,      fn: 'vis.refresh',     activeOn: ['bookmarks-history'], parent: 'probes-settings', order: 0 },
        { id: 'vis-fn-cols',        label: 'Columns',              icon: 'icons/ui/table-columns-blue.svg', fn: 'vis.cols', activeOn: ['bookmarks-history'], parent: 'probes-settings', order: 1 },

        // ── Setup & Import page function items ─────────────────────────────
        { id: 'setup-fn-import',    label: 'Import HTML',         icon: HIEROGLYPHS.papyrus,    fn: 'setup.import',    activeOn: ['bookmarks-setup'],   parent: 'probes-settings', order: 0 },
        { id: 'setup-fn-ext',       label: 'Download extension',   icon: 'icons/ui/arrow-down-blue.svg', fn: 'setup.ext', activeOn: ['bookmarks-setup'], parent: 'probes-settings', order: 1 },
    ],
});

// ── Function registrations ───────────────────────────────────────────────────
// probes-menu.js loads after bookmarks.js so all referenced globals are in scope.
// To register functions for an additional page, call:
//   ProbesMenuConfig.registerFunctions({ 'ns.key': () => myFunction() })
// from any script loaded after probes-menu.js, or add entries here with a
// matching fn item in defaultMenu above (fn: 'ns.key', activeOn: ['tab-id']).

ProbesMenuConfig.registerFunctions({
    // Bookmarks — Main tab
    'bm.add':         () => openBookmarkModal(null),
    'bm.import':      () => document.getElementById('bm-import-file').click(),
    'bm.refresh':     () => loadBookmarks(),
    'bm.cols':        () => _bmOpenColsModal(),
    'bm.explainSort': () => {
        if (!_bmSearchActive) {
            const st = document.getElementById('bm-search-status');
            if (st) {
                st.textContent = 'Explain Sort is only available during an active search.';
                st.hidden = false;
                setTimeout(() => { st.hidden = true; }, 3000);
            }
            return;
        }
        _bmOpenSortExplainModal();
    },
    'bm.deadLinks':   () => _bmAutoArchiveDead(null),

    // pfSense DNS
    'dns.refresh':    () => loadPfSenseDns(),
    'dns.probe':      () => probePfSense(),
    'dns.sweep':      () => pingSweep(),
    'dns.expandAll':  () => setAllDnsGroups(true),
    'dns.collapseAll':() => setAllDnsGroups(false),

    // Proxmox Config
    'pve.refresh':    () => loadProxmoxConfig(),
    'pve.fullProbe':  () => fullProbeProxmox(),
    'pve.steps':      () => togglePveSteps(),
    'pve.expandAll':  () => setAllNets(true),
    'pve.collapseAll':() => setAllNets(false),

    // VLANs
    'vlan.refresh':   () => loadVlans(),

    // SSH Targets
    'ssh.rebuild':    () => rebuildSshTargets(),

    // Dockge Stacks
    'dockge.refresh':    () => loadDockgeStacks(),
    'dockge.probe':      () => probeDockgeStacks(),
    'dockge.expandAll':  () => setAllDockgeServices(true),
    'dockge.collapse':   () => setAllDockgeServices(false),

    // Caddy Configs
    'caddy.refresh':  () => loadCaddyConfigs(),
    'caddy.probe':    () => probeCaddyConfigs(),

    // Visit History
    'vis.refresh':    () => loadVisits(),
    'vis.cols':       () => _visOpenColsModal(),

    // Setup & Import
    'setup.import':   () => { const inp = document.getElementById('bm-import-file2'); if (inp) inp.click(); },
    'setup.ext':      () => _bmDownloadExtension(null),
});
