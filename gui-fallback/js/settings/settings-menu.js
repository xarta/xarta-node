// settings-menu.js — Split-dropdown navigation for the Settings group
// xarta-node Blueprints GUI
//
// Adapts the reference menu-system pattern (patterns/menu-system/) to
// the Settings group. Calls the existing switchTab() / switchGroup()
// infrastructure — does NOT manage its own tab panels.
//
// localStorage key: 'blueprintsSettingsMenuConfig'
// (unique key — probes menu uses 'blueprintsProbesMenuConfig')
//
// Default groupings:
//   🗄 PVE Hosts  [▼ 🤝 Nodes]                        (implicit Fleet group)
//   🔧 App Config [▼ 🗺 Manual ARP, 🤖 AI Providers]  (implicit Configuration group)
//   🗝 Keys       [▼ 🔒 Certs]                         (implicit Security group)
//   📄 Docs       [▼ 📋 Doc List, 🖼️ Images, 🩺 Self Diagnostic]  (implicit Reference group)
//   ☰  (this editor — standalone)
//
// Disambiguation note:
//   "🔧 App Config"  = /api/v1/settings key-value store  (tab id: settings)
//   "☰"             = this menu editor                   (tab id: settings-layout)
//
// No inline event handlers — all event wiring via addEventListener.

'use strict';

const SettingsMenuConfig = {

    STORAGE_KEY: 'blueprintsSettingsMenuConfig',

    _initialized: false,

    // Registry of callable functions assignable to menu items.
    _fnRegistry: {},

    // Register one or more functions by key.
    registerFunctions(map) {
        Object.assign(this._fnRegistry, map);
    },

    // ── Default menu structure ─────────────────────────────────
    // id     — must match existing switchTab() tab IDs
    // parent — null = top-level; 'parentId' = child of that item
    // order  — sort order within level (0-based)
    //
    // All top-level items are real tabs so clicking their label always
    // navigates to a live panel (same pattern as proxmox-config in probes).
    defaultMenu: [
        { id: 'pve-hosts',       label: '🗄 PVE Hosts',       icon: '🗄', pageLabel: 'PVE Hosts',       parent: null,       order: 0 },
        { id: 'nodes',           label: '🤝 Nodes',           icon: '🤝', pageLabel: 'Fleet Nodes',     parent: 'pve-hosts', order: 0 },
        { id: 'settings',        label: '🔧 App Config',      icon: '🔧', pageLabel: 'App Config',      parent: null,       order: 1 },
        { id: 'arp-manual',      label: '🗺 Manual ARP',      icon: '🗺', pageLabel: 'Manual ARP',      parent: 'settings',  order: 0 },
        { id: 'ai-providers',    label: '🤖 AI Providers',    icon: '🤖', pageLabel: 'AI Providers',    parent: 'settings',  order: 1 },
        { id: 'keys',            label: '🗝 Keys',            icon: '🗝', pageLabel: 'SSH Keys',        parent: null,       order: 2 },
        { id: 'certs',           label: '🔒 Certs',           icon: '🔒', pageLabel: 'Certificates',    parent: 'keys',     order: 0 },
        { id: 'docs',            label: '📄 Docs',            icon: '📄', pageLabel: 'Docs',            parent: null,       order: 3 },
        { id: 'docs-list',       label: '📋 Doc List',        icon: '📋', pageLabel: 'Doc List',        parent: 'docs',     order: 0 },
        { id: 'docs-images',     label: '🖼️ Images',          icon: '🖼️', pageLabel: 'Doc Images',      parent: 'docs',     order: 1 },
        { id: 'self-diag',       label: '🩺 Self Diagnostic', icon: '🩺', pageLabel: 'Self Diagnostic', parent: 'docs',     order: 2 },
        { id: 'settings-layout', label: '☰',                  icon: '☰',  pageLabel: 'Navbar Layout',   parent: null,       order: 4 },

        // ── PVE Hosts page function items ─────────────────────────────
        { id: 'pveh-fn-refresh', label: '↺ Refresh',          icon: '↺', fn: 'pveh.refresh', activeOn: ['pve-hosts'], parent: 'settings-layout', order: 0 },
        { id: 'pveh-fn-scan',    label: '▶ Scan for Proxmox', icon: '▶', fn: 'pveh.scan',    activeOn: ['pve-hosts'], parent: 'settings-layout', order: 1 },

        // ── Fleet Nodes page function items ──────────────────────────
        { id: 'nod-fn-refresh',  label: '↺ Refresh',          icon: '↺', fn: 'nod.refresh',  activeOn: ['nodes'], parent: 'settings-layout', order: 0 },
        { id: 'nod-fn-update',   label: '▲ Fleet Update',     icon: '▲', fn: 'nod.update',   activeOn: ['nodes'], parent: 'settings-layout', order: 1 },

        // ── App Config page function items ───────────────────────────
        { id: 'cfg-fn-add',      label: '➕ Add setting',      icon: '➕', fn: 'cfg.add',      activeOn: ['settings'], parent: 'settings-layout', order: 0 },
        { id: 'cfg-fn-refresh',  label: '↺ Refresh',          icon: '↺', fn: 'cfg.refresh',  activeOn: ['settings'], parent: 'settings-layout', order: 1 },
        { id: 'cfg-fn-cache',    label: '↺ Refresh cache',    icon: '↺', fn: 'cfg.cache',    activeOn: ['settings'], parent: 'settings-layout', order: 2 },

        // ── Manual ARP page function items ───────────────────────────
        { id: 'arp-fn-add',      label: '➕ Add entry',        icon: '➕', fn: 'arp.add',      activeOn: ['arp-manual'], parent: 'settings-layout', order: 0 },
        { id: 'arp-fn-refresh',  label: '↺ Refresh',          icon: '↺', fn: 'arp.refresh',  activeOn: ['arp-manual'], parent: 'settings-layout', order: 1 },

        // ── AI Providers page function items ──────────────────────────
        { id: 'ai-fn-addprov',   label: '➕ Add provider',     icon: '➕', fn: 'ai.addProv',   activeOn: ['ai-providers'], parent: 'settings-layout', order: 0 },
        { id: 'ai-fn-refresh',   label: '↺ Refresh',          icon: '↺', fn: 'ai.refresh',   activeOn: ['ai-providers'], parent: 'settings-layout', order: 1 },
        { id: 'ai-fn-addassign', label: '➕ Add assignment',   icon: '➕', fn: 'ai.addAssign', activeOn: ['ai-providers'], parent: 'settings-layout', order: 2 },

        // ── Docs page function items ───────────────────────────────────
        { id: 'doc-fn-reload',   label: '↺ Reload',           icon: '↺', fn: 'doc.reload',   activeOn: ['docs'], parent: 'settings-layout', order: 0 },
        { id: 'doc-fn-new',      label: '➕ New Doc',          icon: '➕', fn: 'doc.new',      activeOn: ['docs'], parent: 'settings-layout', order: 1 },
        { id: 'doc-fn-preview',  label: '👁 Edit / Preview',  icon: '👁', fn: 'doc.preview',  activeOn: ['docs'], parent: 'settings-layout', order: 2 },
        { id: 'doc-fn-save',     label: '💾 Save',            icon: '💾', fn: 'doc.save',     activeOn: ['docs'], parent: 'settings-layout', order: 3 },
        { id: 'doc-fn-meta',     label: '✎ Meta',             icon: '✎', fn: 'doc.meta',     activeOn: ['docs'], parent: 'settings-layout', order: 4 },
        { id: 'doc-fn-delete',   label: '🗑 Delete',          icon: '🗑', fn: 'doc.delete',   activeOn: ['docs'], parent: 'settings-layout', order: 5 },

        // ── Doc List page function items ───────────────────────────────
        { id: 'dlist-fn-addgrp', label: '➕ Add Group',        icon: '➕', fn: 'dlist.addGrp', activeOn: ['docs-list'], parent: 'settings-layout', order: 0 },

        // ── Self Diagnostic page function items ────────────────────────
        { id: 'diag-fn-run',     label: '▶ Run Diagnostics',  icon: '▶', fn: 'diag.run',     activeOn: ['self-diag'], parent: 'settings-layout', order: 0 },
    ],

    currentMenu: [],
    _activeId: null,
    // Last content tab visited before the layout editor was opened.
    // Used to drive fn-item context dimming inside the editor.
    _lastContentId: null,
    draggedItem: null,

    // ── Lifecycle ──────────────────────────────────────────────

    // Called by switchGroup('settings') each time the settings group becomes active.
    // Full setup on first call; subsequent calls just refresh active state.
    showGroup() {
        if (!this._initialized) {
            this.loadConfig();
            this.renderEditor();
            this.setupDragAndDrop();
            this._initialized = true;

            // Wire hamburger toggle
            const toggle = document.getElementById('settingsMenuToggle');
            if (toggle) toggle.addEventListener('click', () => this.toggleMenu());

            // Wire save/reset buttons in the layout editor panel
            const saveBtn = document.getElementById('settingsMenuSaveButton');
            if (saveBtn) saveBtn.addEventListener('click', () => this.saveConfig(true));
            const resetBtn = document.getElementById('settingsMenuResetButton');
            if (resetBtn) resetBtn.addEventListener('click', () => this.resetConfig());
        }
        // Always refresh active state when group re-activates (also renders navbar)
        this.updateActiveTab();
    },

    // ── Persistence ────────────────────────────────────────────

    loadConfig() {
        const saved = localStorage.getItem(this.STORAGE_KEY);
        if (saved) {
            try {
                this.currentMenu = JSON.parse(saved);
                // Upgrade migration: auto-add items missing from older saves, and back-fill new fields
                this.defaultMenu.forEach(def => {
                    const existing = this.currentMenu.find(m => m.id === def.id);
                    if (!existing) {
                        this.currentMenu.push({ ...def });
                    } else {
                        // Back-fill fields that may be missing from older saved configs
                        if (existing.pageLabel === undefined) existing.pageLabel = def.pageLabel;
                        // fn and activeOn are always developer-controlled — always sync from defaultMenu
                        if (def.fn !== undefined) existing.fn = def.fn; else delete existing.fn;
                        if (def.activeOn !== undefined) existing.activeOn = def.activeOn; else delete existing.activeOn;
                    }
                });
            } catch (e) {
                console.error('[SettingsMenuConfig] Failed to parse saved config:', e);
                this.currentMenu = JSON.parse(JSON.stringify(this.defaultMenu));
            }
        } else {
            this.currentMenu = JSON.parse(JSON.stringify(this.defaultMenu));
        }
    },

    saveConfig(syncFromDOM = true) {
        if (syncFromDOM) this.updateOrderFromDOM();
        localStorage.setItem(this.STORAGE_KEY, JSON.stringify(this.currentMenu));
        this.renderNavbar();
        this.showSaveNotification();
    },

    resetConfig() {
        if (confirm('Reset settings navbar to default layout?')) {
            localStorage.removeItem(this.STORAGE_KEY);
            this.currentMenu = JSON.parse(JSON.stringify(this.defaultMenu));
            this.saveConfig(false);
            this.renderEditor();
            this.setupDragAndDrop();
        }
    },

    showSaveNotification() {
        const notif = document.getElementById('settingsMenuSaveNotification');
        if (notif) {
            notif.classList.add('show');
            setTimeout(() => notif.classList.remove('show'), 2000);
        }
    },

    // ── Data helpers ───────────────────────────────────────────

    getTopLevelItems() {
        return this.currentMenu
            .filter(m => !m.parent)
            .sort((a, b) => a.order - b.order);
    },

    getChildren(parentId) {
        return this.currentMenu
            .filter(m => m.parent === parentId)
            .sort((a, b) => a.order - b.order);
    },

    updateOrderFromDOM() {
        const topItems = document.querySelectorAll('#settingsMenuEditorList > .menu-editor-item');
        topItems.forEach((el, idx) => {
            const id = el.dataset.id;
            const item = this.currentMenu.find(m => m.id === id);
            if (item) { item.order = idx; item.parent = null; }

            const children = el.querySelectorAll('.menu-editor-children > .menu-editor-item');
            children.forEach((childEl, childIdx) => {
                const childId = childEl.dataset.id;
                const childItem = this.currentMenu.find(m => m.id === childId);
                if (childItem) { childItem.order = childIdx; childItem.parent = id; }
            });
        });
    },

    // ── Mobile menu ────────────────────────────────────────────

    toggleMenu() {
        const toggle = document.getElementById('settingsMenuToggle');
        const tabs   = document.getElementById('settingsHubTabs');
        if (!toggle || !tabs) return;
        const open = tabs.classList.toggle('open');
        toggle.classList.toggle('open', open);
    },

    closeMenu() {
        const toggle = document.getElementById('settingsMenuToggle');
        const tabs   = document.getElementById('settingsHubTabs');
        if (toggle) toggle.classList.remove('open');
        if (tabs)   tabs.classList.remove('open');
    },

    // ── Navbar rendering ───────────────────────────────────────

    renderNavbar(activeId) {
        if (activeId === undefined) activeId = this._activeId;
        const navbar = document.getElementById('settingsHubTabs');
        if (!navbar) return;
        navbar.innerHTML = '';

        this.getTopLevelItems().forEach(item => {
            const children     = this.getChildren(item.id);
            const navChildren  = children.filter(c => !c.fn);
            const fnChildren   = children.filter(c => !!c.fn);

            // Function children filtered by activeOn context.
            const visibleFnChildren = fnChildren.filter(c =>
                !c.activeOn || (activeId && c.activeOn.includes(activeId))
            );

            const allNavGroup  = [item, ...navChildren];
            const activeMember = activeId ? allNavGroup.find(m => m.id === activeId) : null;
            const isGroupActive = !!activeMember;

            const dropdownNavItems = navChildren.length > 0
                ? (isGroupActive
                    ? allNavGroup.filter(m => m.id !== activeMember.id)
                    : navChildren)
                : [];

            const allDropdownItems = [...dropdownNavItems, ...visibleFnChildren];

            const labelText = isGroupActive
                ? (activeMember.pageLabel || activeMember.label)
                : item.label;

            if (allDropdownItems.length > 0) {
                const hasSeparator = dropdownNavItems.length > 0 && visibleFnChildren.length > 0;
                const navHtml  = dropdownNavItems.map(c =>
                    `<button class="hub-dropdown-item" data-tab="${c.id}">${c.label}</button>`
                ).join('');
                const sepHtml  = hasSeparator ? '<hr class="hub-dropdown-separator">' : '';
                const fnHtml   = visibleFnChildren.map(c =>
                    `<button class="hub-dropdown-item hub-dropdown-fn" data-fn="${c.fn}">${c.label}</button>`
                ).join('');

                const isActive = isGroupActive || (activeId === item.id);
                const dropdown = document.createElement('div');
                dropdown.className = 'hub-tab-dropdown';
                dropdown.innerHTML = `
                    <div class="hub-tab-split">
                        <button class="hub-tab hub-tab-label${isActive ? ' active' : ''}" data-tab="${item.id}">${labelText}</button>
                        <button class="hub-tab-caret" aria-label="Toggle submenu">▼</button>
                    </div>
                    <div class="hub-dropdown-menu">
                        ${navHtml}${sepHtml}${fnHtml}
                    </div>
                `;

                dropdown.querySelector('.hub-tab-label').addEventListener('click', (e) => {
                    e.stopPropagation();
                    const targetId = isGroupActive
                        ? activeMember.id
                        : (document.getElementById('tab-' + item.id) ? item.id : navChildren[0]?.id);
                    if (targetId) {
                        switchTab(targetId);
                        this.updateActiveTab(targetId);
                        this.closeMenu();
                        this.closeDropdowns();
                    }
                });

                dropdown.querySelector('.hub-tab-caret').addEventListener('click', (e) => {
                    e.stopPropagation();
                    const wasOpen = dropdown.classList.contains('open');
                    this.closeDropdowns();
                    if (!wasOpen) dropdown.classList.add('open');
                });

                dropdown.querySelectorAll('.hub-dropdown-item:not(.hub-dropdown-fn)').forEach(btn => {
                    btn.addEventListener('click', (e) => {
                        e.stopPropagation();
                        const destId = btn.dataset.tab;
                        const panel = document.getElementById('tab-' + destId);
                        const destChildren = this.getChildren(destId);
                        const targetId = panel ? destId : (destChildren[0]?.id || destId);
                        switchTab(targetId);
                        this.updateActiveTab(targetId);
                        this.closeMenu();
                        this.closeDropdowns();
                    });
                });

                dropdown.querySelectorAll('.hub-dropdown-fn').forEach(btn => {
                    btn.addEventListener('click', (e) => {
                        e.stopPropagation();
                        const fn = this._fnRegistry[btn.dataset.fn];
                        if (typeof fn === 'function') fn();
                        else console.warn('[SettingsMenuConfig] No function registered for:', btn.dataset.fn);
                        this.closeMenu();
                        this.closeDropdowns();
                    });
                });

                navbar.appendChild(dropdown);

            } else if (item.fn) {
                const btn = document.createElement('button');
                btn.className = 'hub-tab';
                btn.textContent = item.label;
                btn.addEventListener('click', () => {
                    const fn = this._fnRegistry[item.fn];
                    if (typeof fn === 'function') fn();
                    else console.warn('[SettingsMenuConfig] No function registered for:', item.fn);
                    this.closeMenu();
                });
                navbar.appendChild(btn);

            } else {
                const btn = document.createElement('button');
                btn.className = 'hub-tab';
                btn.dataset.tab = item.id;
                btn.textContent = item.label;
                if (isGroupActive) btn.classList.add('active');
                btn.addEventListener('click', () => {
                    switchTab(item.id);
                    this.updateActiveTab(item.id);
                    this.closeMenu();
                });
                navbar.appendChild(btn);
            }
        });

        document.removeEventListener('click', this._closeHandler);
        this._closeHandler = () => this.closeDropdowns();
        document.addEventListener('click', this._closeHandler);
    },

    closeDropdowns() {
        document.querySelectorAll('#settingsHubTabs .hub-tab-dropdown.open')
            .forEach(d => d.classList.remove('open'));
    },

    // Update active visual state. Accepts an explicit tabId or derives from DOM.
    updateActiveTab(activeId) {
        if (!activeId) {
            const activePanel = document.querySelector('.tab-panel.active');
            if (activePanel) activeId = activePanel.id.replace('tab-', '');
        }
        if (activeId) this._activeId = activeId;

        // Track the last *content* page (not the layout editor itself) so that
        // fn-item context badges stay meaningful while the editor is open.
        const isLayoutEditorItem = this.defaultMenu.some(
            m => m.parent === activeId && m.fn !== undefined
        );
        if (activeId && !isLayoutEditorItem) this._lastContentId = activeId;

        // Update mobile hamburger label
        const labelEl = document.getElementById('settingsCurrentTabLabel');
        if (labelEl && activeId) {
            const item = this.currentMenu.find(m => m.id === activeId);
            if (item) labelEl.textContent = item.pageLabel || item.label;
        }

        // Re-render navbar with active state baked in
        this.renderNavbar(activeId || this._activeId);
        // Re-render editor so fn item context badges update as the active tab changes.
        this.renderEditor();
        this.setupDragAndDrop();
    },

    // ── Editor rendering ───────────────────────────────────────

    renderEditor() {
        const container = document.getElementById('settingsMenuEditorList');
        if (!container) return;
        container.innerHTML = '';
        this.getTopLevelItems().forEach(item => container.appendChild(this.createEditorItem(item)));
    },

    createEditorItem(item) {
        const div = document.createElement('div');
        div.className = 'menu-editor-item';
        div.dataset.id = item.id;
        div.draggable = true;

        const children = this.getChildren(item.id);
        const hasChildren = children.length > 0;

        div.innerHTML = `
            <div class="menu-item-header">
                <span class="drag-handle">⋮⋮</span>
                <span class="menu-item-icon">${item.icon}</span>
                <span class="menu-item-label">${item.label.replace(item.icon, '').trim()}</span>
                ${hasChildren ? '<span class="has-children-badge">▼ ' + children.length + '</span>' : ''}
                <span class="menu-item-page-label" title="Page label (shown when active)">→ ${item.pageLabel || '—'}</span>
                <div class="menu-item-actions">
                    <button class="btn-edit-item" data-id="${item.id}" title="Edit nav label">✏️</button>
                    <button class="btn-edit-page-label" data-id="${item.id}" title="Edit page label">🏷️</button>
                </div>
            </div>
            <div class="menu-editor-children" data-parent="${item.id}">
                ${children.map(child => {
                    const isFn = !!child.fn;
                    const defItem = this.defaultMenu.find(m => m.id === child.id);
                    const fnKey = defItem?.fn || child.fn || '';
                    const activeOnArr = (isFn && defItem?.activeOn) ? defItem.activeOn : null;

                    const isLayoutEditor = this._activeId === item.id;
                    const contextId = isLayoutEditor ? this._lastContentId : this._activeId;
                    const isInContext = !activeOnArr
                        || (isLayoutEditor && !this._lastContentId)
                        || Boolean(contextId && activeOnArr.includes(contextId));

                    const tabList = activeOnArr ? activeOnArr.join(' / ') : '';
                    const badgeTitle = isInContext
                        ? `Active — visible in dropdown now`
                        : `Inactive — visible in dropdown only when on: ${tabList}`;
                    const contextBadgeHtml = activeOnArr
                        ? `<span class="menu-fn-context-badge${isInContext && contextId ? ' is-active' : ''}" title="${badgeTitle}">● ${tabList}</span>`
                        : '';

                    const rightColHtml = isFn
                        ? `<span class="menu-fn-badge" title="Function — ${fnKey}">⚡ ${fnKey}</span>${contextBadgeHtml}`
                        : `<span class="menu-item-page-label" title="Page label">→ ${child.pageLabel || '—'}</span>`;
                    const editPageBtnHtml = isFn ? ''
                        : `<button class="btn-edit-page-label" data-id="${child.id}" title="Edit page label">🏷️</button>`;
                    const inactiveClass = (isFn && !isInContext) ? ' menu-editor-fn-child--inactive' : '';
                    return `
                    <div class="menu-editor-item menu-editor-child${isFn ? ' menu-editor-fn-child' : ''}${inactiveClass}" data-id="${child.id}" draggable="true">
                        <div class="menu-item-header">
                            <span class="drag-handle">⋮⋮</span>
                            <span class="menu-item-icon">${child.icon}</span>
                            <span class="menu-item-label">${child.label.replace(child.icon, '').trim()}</span>
                            ${rightColHtml}
                            <div class="menu-item-actions">
                                ${editPageBtnHtml}
                                <button class="btn-promote-item" data-id="${child.id}" title="Promote to top level">⬆️</button>
                            </div>
                        </div>
                    </div>`;
                }).join('')}
                <div class="drop-zone-child" data-parent="${item.id}">
                    <span>Drop here to nest as submenu item</span>
                </div>
            </div>
        `;

        // Wire buttons (no inline handlers — CSP-safe)
        div.querySelector('.btn-edit-item').addEventListener('click', () => this.editItem(item.id));
        div.querySelectorAll('.btn-edit-page-label').forEach(btn => {
            btn.addEventListener('click', () => this.editPageLabel(btn.dataset.id));
        });
        div.querySelectorAll('.btn-promote-item').forEach(btn => {
            btn.addEventListener('click', () => this.promoteItem(btn.dataset.id));
        });

        return div;
    },

    editItem(id) {
        const item = this.currentMenu.find(m => m.id === id);
        if (!item) return;
        const newLabel = prompt('Enter new nav label (without emoji):', item.label.replace(item.icon, '').trim());
        if (newLabel !== null && newLabel.trim()) {
            item.label = item.icon + ' ' + newLabel.trim();
            this.saveConfig(false);
            this.renderEditor();
            this.setupDragAndDrop();
        }
    },

    editPageLabel(id) {
        const item = this.currentMenu.find(m => m.id === id);
        if (!item) return;
        const current = item.pageLabel || item.label.replace(item.icon, '').trim();
        const newLabel = prompt('Enter page label (shown as the active tab indicator):', current);
        if (newLabel !== null && newLabel.trim()) {
            item.pageLabel = newLabel.trim();
            this.saveConfig(false);
            this.renderEditor();
            this.setupDragAndDrop();
        }
    },

    promoteItem(id) {
        const item = this.currentMenu.find(m => m.id === id);
        if (item) {
            item.parent = null;
            item.order = this.getTopLevelItems().length;
            this.saveConfig(false);
            this.renderEditor();
            this.setupDragAndDrop();
        }
    },

    // ── Drag & Drop ────────────────────────────────────────────

    setupDragAndDrop() {
        const container = document.getElementById('settingsMenuEditorList');
        if (!container) return;

        // Replace node to remove all stale event listeners
        const fresh = container.cloneNode(true);
        container.parentNode.replaceChild(fresh, container);

        // Re-wire edit/promote buttons on the cloned tree
        fresh.querySelectorAll('.btn-edit-item').forEach(btn => {
            btn.addEventListener('click', () => this.editItem(btn.dataset.id));
        });
        fresh.querySelectorAll('.btn-edit-page-label').forEach(btn => {
            btn.addEventListener('click', () => this.editPageLabel(btn.dataset.id));
        });
        fresh.querySelectorAll('.btn-promote-item').forEach(btn => {
            btn.addEventListener('click', () => this.promoteItem(btn.dataset.id));
        });

        fresh.addEventListener('dragstart', (e) => {
            const item = e.target.closest('.menu-editor-item');
            if (item) {
                this.draggedItem = item;
                item.classList.add('dragging');
                e.dataTransfer.effectAllowed = 'move';
                e.dataTransfer.setData('text/plain', item.dataset.id);
            }
        });

        fresh.addEventListener('dragend', () => {
            if (this.draggedItem) {
                this.draggedItem.classList.remove('dragging');
                document.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
                this.draggedItem = null;
            }
        });

        fresh.addEventListener('dragover', (e) => {
            e.preventDefault();
            const target = e.target.closest('.menu-editor-item, .drop-zone-child');
            if (target && target !== this.draggedItem) {
                document.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
                target.classList.add('drag-over');
            }
        });

        fresh.addEventListener('dragleave', (e) => {
            const target = e.target.closest('.menu-editor-item, .drop-zone-child');
            if (target) target.classList.remove('drag-over');
        });

        fresh.addEventListener('drop', (e) => {
            e.preventDefault();
            if (!this.draggedItem) return;

            const dropZone  = e.target.closest('.drop-zone-child');
            const targetItem = e.target.closest('.menu-editor-item');

            if (dropZone) {
                // Drop into a sub-menu zone
                const parentId  = dropZone.dataset.parent;
                const draggedId = this.draggedItem.dataset.id;
                if (parentId === draggedId) return;
                if (this.getChildren(draggedId).length > 0) {
                    alert('Cannot nest an item that already has sub-items.');
                    return;
                }
                const item = this.currentMenu.find(m => m.id === draggedId);
                if (item) {
                    item.parent = parentId;
                    item.order  = this.getChildren(parentId).length;
                    this.saveConfig(false);
                    this.renderEditor();
                    this.setupDragAndDrop();
                }

            } else if (targetItem && targetItem !== this.draggedItem) {
                const draggedIsChild = this.draggedItem.classList.contains('menu-editor-child');
                const targetIsChild  = targetItem.classList.contains('menu-editor-child');

                if (draggedIsChild && targetIsChild) {
                    // Reorder children within same parent
                    const draggedParent = this.draggedItem.closest('.menu-editor-children');
                    const targetParent  = targetItem.closest('.menu-editor-children');
                    if (draggedParent && targetParent && draggedParent === targetParent) {
                        const items = Array.from(draggedParent.querySelectorAll(':scope > .menu-editor-item'));
                        const draggedIdx = items.indexOf(this.draggedItem);
                        const targetIdx  = items.indexOf(targetItem);
                        if (draggedIdx !== -1 && targetIdx !== -1) {
                            if (draggedIdx < targetIdx) targetItem.after(this.draggedItem);
                            else                         targetItem.before(this.draggedItem);
                            this.saveConfig(true);
                        }
                    }
                } else if (!draggedIsChild && !targetIsChild) {
                    // Reorder top-level items
                    const list  = document.getElementById('settingsMenuEditorList');
                    const items = Array.from(list.querySelectorAll(':scope > .menu-editor-item'));
                    const draggedIdx = items.indexOf(this.draggedItem);
                    const targetIdx  = items.indexOf(targetItem);
                    if (draggedIdx !== -1 && targetIdx !== -1) {
                        if (draggedIdx < targetIdx) targetItem.after(this.draggedItem);
                        else                         targetItem.before(this.draggedItem);
                        this.saveConfig(true);
                    }
                }
            }

            document.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
        });
    },
};

// ── Built-in function registrations ─────────────────────────────────────────
// settings-menu.js loads after all settings page scripts so all referenced
// globals are in scope. To register functions for an additional page, call:
//   SettingsMenuConfig.registerFunctions({ 'ns.key': () => myFunction() })
// from any script loaded after settings-menu.js, or add entries here.

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
    'doc.preview':  () => docsTogglePreview(),
    'doc.save':     () => docsSave(),
    'doc.meta':     () => openEditDocModal(),
    'doc.delete':   () => openDeleteDocModal(),

    // Doc List
    'dlist.addGrp': () => docsListAddGroup(),

    // Self Diagnostic
    'diag.run':     () => runSelfDiag(),
});
