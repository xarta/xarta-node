// synthesis-menu.js — Split-dropdown navigation for the Synthesis group
// xarta-node Blueprints GUI
//
// Thin wrapper around createHubMenu() (hub-menu.js).
// Contains only the Synthesis-specific config, defaultMenu, and function registrations.
//
// localStorage key: 'blueprintsSynthesisMenuConfig'
//
// Note: 'manual-links-rendered' and 'manual-links-table' are pseudo-tab IDs.
// switchTab() intercepts them to show #tab-manual-links and call manualLinksShowView().
//
// No inline event handlers — all event wiring via addEventListener.

'use strict';

const SynthesisMenuConfig = createHubMenu({
    storageKey:      'blueprintsSynthesisMenuConfig',
    toggleId:        'synthesisMenuToggle',
    tabsId:          'synthesisHubTabs',
    currentLabelId:  'synthesisCurrentTabLabel',
    saveButtonId:    'synthesisMenuSaveButton',
    resetButtonId:   'synthesisMenuResetButton',
    editorListId:    'synthesisMenuEditorList',
    notificationId:  'synthesisMenuSaveNotification',
    resetConfirmMsg: 'Reset synthesis navbar to default layout?',
    // Mobile: the layout/context button is pinned outside the hamburger menu
    mobilePinnedId:  'synthesis-layout',
    pinnedTabsId:    'synthesisHubTabsPinned',
    defaultMenu: [
        { id: 'manual-links',          label: '🔗 Manual',       icon: '🔗', pageLabel: 'Manual Links',          parent: null,              order: 0 },
        { id: 'manual-links-rendered', label: '🌐 Rendered',     icon: '🌐', pageLabel: 'Manual Links',          parent: 'manual-links',    order: 0 },
        { id: 'manual-links-table',    label: '≡ Table',         icon: '≡',  pageLabel: 'Manual Links (Table)',  parent: 'manual-links',    order: 1 },
        { id: 'services',              label: '📋 Services',     icon: '📋', pageLabel: 'Services',              parent: null,              order: 1 },
        { id: 'machines',              label: '🖥 Machines',     icon: '🖥', pageLabel: 'Machines',              parent: null,              order: 2 },
        { id: 'synthesis-layout',      label: '☰',               icon: '☰',  pageLabel: 'Navbar Layout',         parent: null,              order: 3 },

        // ── Services page function items ──────────────────────────────────
        { id: 'svc-fn-add',     label: '➕ Add service', icon: '➕', fn: 'svc.add',     activeOn: ['services'], parent: 'synthesis-layout', order: 0 },
        { id: 'svc-fn-refresh', label: '↺ Refresh',      icon: '↺', fn: 'svc.refresh', activeOn: ['services'], parent: 'synthesis-layout', order: 1 },

        // ── Machines page function items ──────────────────────────────────
        { id: 'mch-fn-refresh', label: '↺ Refresh',      icon: '↺', fn: 'mch.refresh', activeOn: ['machines'], parent: 'synthesis-layout', order: 0 },
    ],
});

// ── Function registrations ───────────────────────────────────────────────────
// synthesis-menu.js loads after services.js and machines.js so all referenced
// globals are in scope.

SynthesisMenuConfig.registerFunctions({
    'svc.add':     () => openAddModal(),
    'svc.refresh': () => loadServices(),
    'mch.refresh': () => loadMachines(),
});
