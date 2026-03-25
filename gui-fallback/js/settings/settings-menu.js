// settings-menu.js — Split-dropdown navigation for the Settings group
// xarta-node Blueprints GUI
//
// Thin wrapper around createHubMenu() (hub-menu.js).
// Contains only the Settings-specific config, defaultMenu, and function registrations.
//
// localStorage key: 'blueprintsSettingsMenuConfig'
//
// Default groupings:
//   🗄 PVE Hosts  [▼ 🤝 Nodes]
//   🔧 App Config [▼ 🗺 Manual ARP, 🤖 AI Providers]
//   🗝 Keys       [▼ 🔒 Certs]
//   📄 Docs       [▼ 📋 Doc List, 🖼️ Images, 🩺 Self Diagnostic]
//   ☰  (layout editor — standalone)
//
// No inline event handlers — all event wiring via addEventListener.

'use strict';

const SettingsMenuConfig = createHubMenu({
    storageKey:      'blueprintsSettingsMenuConfig',
    group:           'settings',
    toggleId:        'settingsMenuToggle',
    tabsId:          'settingsHubTabs',
    currentLabelId:  'settingsCurrentTabLabel',
    saveButtonId:    'settingsMenuSaveButton',
    resetButtonId:   'settingsMenuResetButton',
    editorListId:    'settingsMenuEditorList',
    notificationId:  'settingsMenuSaveNotification',
    resetConfirmMsg: 'Reset settings navbar to default layout?',
    // Mobile: the layout/context button is pinned outside the hamburger menu
    mobilePinnedId:  'settings-layout',
    pinnedTabsId:    'settingsHubTabsPinned',
    defaultMenu: [
        { id: 'pve-hosts',       label: 'PVE Hosts',      icon: 'icons/ui/proxmox-blue.svg', pageLabel: 'PVE Hosts',       parent: null,        order: 0 },
        { id: 'nodes',           label: 'Nodes',          icon: HIEROGLYPHS.crookFlail, pageLabel: 'Fleet Nodes',     parent: 'pve-hosts', order: 0 },
        { id: 'settings',        label: 'App Config',     icon: HIEROGLYPHS.djedPillar, pageLabel: 'App Config',      parent: null,        order: 1 },
        { id: 'arp-manual',      label: 'Manual ARP',     icon: HIEROGLYPHS.obelisk,    pageLabel: 'Manual ARP',      parent: 'settings',  order: 0 },
        { id: 'ai-providers',    label: 'AI Providers',   icon: HIEROGLYPHS.falcon,     pageLabel: 'AI Providers',    parent: 'settings',  order: 1 },
        { id: 'nav-items',       label: 'Nav Items',      icon: HIEROGLYPHS.naosShrine, pageLabel: 'Nav Items',       parent: 'settings',  order: 2 },
        { id: 'form-controls',   label: 'Form Controls',  icon: HIEROGLYPHS.adze,       pageLabel: 'Form Controls',   parent: 'settings',  order: 3 },
        { id: 'keys',            label: 'Keys',           icon: HIEROGLYPHS.ankh,       pageLabel: 'SSH Keys',        parent: null,        order: 2 },
        { id: 'certs',           label: 'Certs',          icon: HIEROGLYPHS.shen,       pageLabel: 'Certificates',    parent: 'keys',      order: 0 },
        { id: 'docs',            label: 'Docs',           icon: HIEROGLYPHS.papyrus,    pageLabel: 'Docs',            parent: null,        order: 3 },
        { id: 'docs-list',       label: 'Doc List',       icon: HIEROGLYPHS.papyrus,    pageLabel: 'Doc List',        parent: 'docs',      order: 0 },
        { id: 'docs-images',     label: 'Images',         icon: HIEROGLYPHS.lotus,      pageLabel: 'Doc Images',      parent: 'docs',      order: 1 },
        { id: 'self-diag',       label: 'Self Diagnostic',icon: HIEROGLYPHS.eyeOfHorus, pageLabel: 'Self Diagnostic', parent: 'docs',      order: 2 },
        { id: 'settings-layout', label: '☰',              icon: HIEROGLYPHS.kheper,     pageLabel: 'Navbar Layout',   parent: null,        order: 4 },

        // ── PVE Hosts page function items ─────────────────────────────────
        { id: 'pveh-fn-refresh', label: 'Refresh',          icon: HIEROGLYPHS.nefer,      fn: 'pveh.refresh', activeOn: ['pve-hosts'],    parent: 'settings-layout', order: 0 },
        { id: 'pveh-fn-scan',    label: 'Scan for Proxmox', icon: HIEROGLYPHS.wasScepter, fn: 'pveh.scan',    activeOn: ['pve-hosts'],    parent: 'settings-layout', order: 1 },

        // ── Fleet Nodes page function items ───────────────────────────────
        { id: 'nod-fn-refresh',  label: 'Refresh',          icon: HIEROGLYPHS.nefer,      fn: 'nod.refresh',  activeOn: ['nodes'],        parent: 'settings-layout', order: 0 },
        { id: 'nod-fn-update',   label: 'Fleet Update',     icon: HIEROGLYPHS.crookFlail, fn: 'nod.update',   activeOn: ['nodes'],        parent: 'settings-layout', order: 1 },

        // ── App Config page function items ────────────────────────────────
        { id: 'cfg-fn-add',      label: 'Add setting',      icon: HIEROGLYPHS.djedPillar, fn: 'cfg.add',      activeOn: ['settings'],     parent: 'settings-layout', order: 0 },
        { id: 'cfg-fn-refresh',  label: 'Refresh',          icon: HIEROGLYPHS.nefer,      fn: 'cfg.refresh',  activeOn: ['settings'],     parent: 'settings-layout', order: 1 },
        { id: 'cfg-fn-cache',    label: 'Refresh cache',    icon: HIEROGLYPHS.nefer,      fn: 'cfg.cache',    activeOn: ['settings'],     parent: 'settings-layout', order: 2 },

        // ── Manual ARP page function items ────────────────────────────────
        { id: 'arp-fn-add',      label: 'Add entry',        icon: HIEROGLYPHS.obelisk,    fn: 'arp.add',      activeOn: ['arp-manual'],   parent: 'settings-layout', order: 0 },
        { id: 'arp-fn-refresh',  label: 'Refresh',          icon: HIEROGLYPHS.nefer,      fn: 'arp.refresh',  activeOn: ['arp-manual'],   parent: 'settings-layout', order: 1 },

        // ── AI Providers page function items ──────────────────────────────
        { id: 'ai-fn-addprov',   label: 'Add provider',     icon: HIEROGLYPHS.falcon,     fn: 'ai.addProv',   activeOn: ['ai-providers'], parent: 'settings-layout', order: 0 },
        { id: 'ai-fn-refresh',   label: 'Refresh',          icon: HIEROGLYPHS.nefer,      fn: 'ai.refresh',   activeOn: ['ai-providers'], parent: 'settings-layout', order: 1 },
        { id: 'ai-fn-addassign', label: 'Add assignment',   icon: HIEROGLYPHS.falcon,     fn: 'ai.addAssign', activeOn: ['ai-providers'], parent: 'settings-layout', order: 2 },

        // ── Docs page function items ───────────────────────────────────────
        { id: 'doc-fn-reload',   label: 'Reload',           icon: HIEROGLYPHS.nefer,      fn: 'doc.reload',   activeOn: ['docs'],         parent: 'settings-layout', order: 0 },
        { id: 'doc-fn-new',      label: 'New Doc',          icon: HIEROGLYPHS.papyrus,    fn: 'doc.new',      activeOn: ['docs'],         parent: 'settings-layout', order: 1 },
        { id: 'doc-fn-add',      label: 'Add Existing',     icon: 'icons/ui/group-folder-blue.svg', fn: 'doc.add', activeOn: ['docs'], parent: 'settings-layout', order: 2 },
        { id: 'doc-fn-preview',  label: 'Edit / Preview',   icon: HIEROGLYPHS.khaHorizon, fn: 'doc.preview',  activeOn: ['docs'],         parent: 'settings-layout', order: 3 },
        { id: 'doc-fn-save',     label: 'Save',             icon: HIEROGLYPHS.tjet,       fn: 'doc.save',     activeOn: ['docs'],         parent: 'settings-layout', order: 4 },
        { id: 'doc-fn-meta',     label: 'Meta',             icon: HIEROGLYPHS.papyrus,    fn: 'doc.meta',     activeOn: ['docs'],         parent: 'settings-layout', order: 5 },
        { id: 'doc-fn-delete',   label: 'Delete',           icon: 'icons/ui/trash-blue.svg', fn: 'doc.delete', activeOn: ['docs'], parent: 'settings-layout', order: 6 },

        // ── Doc List page function items ───────────────────────────────────
        { id: 'dlist-fn-addgrp', label: 'Add Group',        icon: HIEROGLYPHS.papyrus,    fn: 'dlist.addGrp', activeOn: ['docs-list'],    parent: 'settings-layout', order: 0 },

        // ── Self Diagnostic page function items ────────────────────────────
        { id: 'diag-fn-run',     label: 'Run Diagnostics',  icon: HIEROGLYPHS.wasScepter, fn: 'diag.run',     activeOn: ['self-diag'],    parent: 'settings-layout', order: 0 },

        // ── Nav Items page function items ──────────────────────────────────
        { id: 'ni-fn-refresh',   label: 'Refresh',           icon: HIEROGLYPHS.nefer,     fn: 'ni.refresh',   activeOn: ['nav-items'],    parent: 'settings-layout', order: 0 },
        // ── Form Controls page function items ────────────────────────────────
        { id: 'fc-fn-refresh',   label: 'Refresh',           icon: HIEROGLYPHS.nefer,               fn: 'fc.refresh', activeOn: ['form-controls'], parent: 'settings-layout', order: 0 },
        { id: 'fc-fn-add',       label: 'Add Key',           icon: 'icons/ui/plus-blue.svg',        fn: 'fc.add',     activeOn: ['form-controls'], parent: 'settings-layout', order: 1 },
    ],
});

// ── Function registrations ───────────────────────────────────────────────────
// settings-menu.js loads after all settings page scripts so all referenced
// globals are in scope.

SettingsMenuConfig.registerFunctions({
    // PVE Hosts
    'pveh.refresh': () => loadPveHosts(),
    'pveh.scan':    () => scanPveHosts(),

    // Fleet Nodes
    'nod.refresh':  () => loadNodes(),
    'nod.update':   () => {
        if (!confirm('Trigger git pull (public + private repos) on this node and queue for all fleet peers?\n\nAll nodes will pull latest code and restart if there are new commits.')) return;
        const statusEl = document.getElementById('fleet-update-status');
        if (statusEl) { statusEl.textContent = '⏳ Updating…'; statusEl.style.color = ''; }
        apiFetch('/api/v1/sync/git-pull', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ scope: 'both' }),
        }).then(r => {
            if (r.ok) {
                if (statusEl) { statusEl.textContent = '✓ Queued for all nodes'; statusEl.style.color = 'var(--ok,#3fb950)'; }
                setTimeout(() => { loadNodes(); }, 4000);
            } else {
                if (statusEl) { statusEl.textContent = `✗ HTTP ${r.status}`; statusEl.style.color = 'var(--danger,#f85149)'; }
            }
        }).catch(e => {
            if (statusEl) { statusEl.textContent = `✗ ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
        }).finally(() => {
            setTimeout(() => { if (statusEl) { statusEl.textContent = ''; statusEl.style.color = ''; } }, 10000);
        });
    },

    // App Config
    'cfg.add':      () => openAddSettingModal(),
    'cfg.refresh':  () => loadSettings(),
    'cfg.cache':    () => refreshFrontendSettingsCache(),

    // Manual ARP
    'arp.add':      () => addArpManualEntry(),
    'arp.refresh':  () => loadArpManual(),

    // AI Providers
    'ai.addProv':   () => openAiProviderModal(null),
    'ai.refresh':   () => loadAiProviders(),
    'ai.addAssign': () => openAiAssignmentModal(null),

    // Docs
    'doc.reload':   () => docsRefreshContent(),
    'doc.new':      () => openNewDocModal(),
    'doc.add':      () => openAddDocModal(),
    'doc.preview':  () => docsTogglePreview(),
    'doc.save':     () => docsSave(),
    'doc.meta':     () => openEditDocModal(),
    'doc.delete':   () => openDeleteDocModal(),

    // Doc List
    'dlist.addGrp': () => docsListAddGroup(),

    // Self Diagnostic
    'diag.run':     () => runSelfDiag(),

    // Nav Items
    'ni.refresh':   () => loadNavItems(),

    // Form Controls
    'fc.refresh':   () => loadFormControls(),
    'fc.add':       () => _fcAddNew(),
});
