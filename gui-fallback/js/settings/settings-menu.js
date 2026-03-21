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
//   📄 Docs       [▼ 🩺 Self Diagnostic]               (implicit Reference group)
//   ☰ Navbar Layout  (this editor — standalone)
//
// Disambiguation note:
//   "🔧 App Config"   = /api/v1/settings key-value store  (tab id: settings)
//   "☰ Navbar Layout" = this menu editor                   (tab id: settings-layout)
//
// No inline event handlers — all event wiring via addEventListener.

'use strict';

const SettingsMenuConfig = {

    STORAGE_KEY: 'blueprintsSettingsMenuConfig',

    _initialized: false,

    // ── Default menu structure ─────────────────────────────────
    // id     — must match existing switchTab() tab IDs
    // parent — null = top-level; 'parentId' = child of that item
    // order  — sort order within level (0-based)
    //
    // All top-level items are real tabs so clicking their label always
    // navigates to a live panel (same pattern as proxmox-config in probes).
    defaultMenu: [
        { id: 'pve-hosts',       label: '🗄 PVE Hosts',       icon: '🗄', parent: null,       order: 0 },
        { id: 'nodes',           label: '🤝 Nodes',           icon: '🤝', parent: 'pve-hosts', order: 0 },
        { id: 'settings',        label: '🔧 App Config',      icon: '🔧', parent: null,       order: 1 },
        { id: 'arp-manual',      label: '🗺 Manual ARP',      icon: '🗺', parent: 'settings',  order: 0 },
        { id: 'ai-providers',    label: '🤖 AI Providers',    icon: '🤖', parent: 'settings',  order: 1 },
        { id: 'keys',            label: '🗝 Keys',            icon: '🗝', parent: null,       order: 2 },
        { id: 'certs',           label: '🔒 Certs',           icon: '🔒', parent: 'keys',     order: 0 },
        { id: 'docs',            label: '📄 Docs',            icon: '📄', parent: null,       order: 3 },
        { id: 'self-diag',       label: '🩺 Self Diagnostic', icon: '🩺', parent: 'docs',     order: 0 },
        { id: 'settings-layout', label: '☰ Navbar Layout',   icon: '☰',  parent: null,       order: 4 },
    ],

    currentMenu: [],
    draggedItem: null,

    // ── Lifecycle ──────────────────────────────────────────────

    // Called by switchGroup('settings') each time the settings group becomes active.
    // Full setup on first call; subsequent calls just refresh active state.
    showGroup() {
        if (!this._initialized) {
            this.loadConfig();
            this.renderNavbar();
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
        // Always refresh active state when group re-activates
        this.updateActiveTab();
    },

    // ── Persistence ────────────────────────────────────────────

    loadConfig() {
        const saved = localStorage.getItem(this.STORAGE_KEY);
        if (saved) {
            try {
                this.currentMenu = JSON.parse(saved);
                // Upgrade migration: auto-add items missing from older saves
                this.defaultMenu.forEach(def => {
                    if (!this.currentMenu.find(m => m.id === def.id)) {
                        this.currentMenu.push({ ...def });
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

    renderNavbar() {
        const navbar = document.getElementById('settingsHubTabs');
        if (!navbar) return;
        navbar.innerHTML = '';

        this.getTopLevelItems().forEach(item => {
            const children = this.getChildren(item.id);

            if (children.length > 0) {
                // Split-button dropdown
                const dropdown = document.createElement('div');
                dropdown.className = 'hub-tab-dropdown';
                dropdown.innerHTML = `
                    <div class="hub-tab-split">
                        <button class="hub-tab hub-tab-label" data-tab="${item.id}">${item.label}</button>
                        <button class="hub-tab-caret" aria-label="Toggle submenu">▼</button>
                    </div>
                    <div class="hub-dropdown-menu">
                        ${children.map(c => `<button class="hub-dropdown-item" data-tab="${c.id}">${c.label}</button>`).join('')}
                    </div>
                `;

                // Label click → navigate to this item's own tab
                // (all top-level items are real tabs in the default config)
                dropdown.querySelector('.hub-tab-label').addEventListener('click', (e) => {
                    e.stopPropagation();
                    const ownPanel = document.getElementById('tab-' + item.id);
                    const targetId = ownPanel ? item.id : children[0].id;
                    switchTab(targetId);
                    this.updateActiveTab(targetId);
                    this.closeMenu();
                    this.closeDropdowns();
                });

                // Caret → toggle submenu open/close
                dropdown.querySelector('.hub-tab-caret').addEventListener('click', (e) => {
                    e.stopPropagation();
                    const wasOpen = dropdown.classList.contains('open');
                    this.closeDropdowns();
                    if (!wasOpen) dropdown.classList.add('open');
                });

                // Child items → navigate
                dropdown.querySelectorAll('.hub-dropdown-item').forEach(btn => {
                    btn.addEventListener('click', (e) => {
                        e.stopPropagation();
                        switchTab(btn.dataset.tab);
                        this.updateActiveTab(btn.dataset.tab);
                        this.closeMenu();
                        this.closeDropdowns();
                    });
                });

                navbar.appendChild(dropdown);

            } else {
                // Plain tab button
                const btn = document.createElement('button');
                btn.className = 'hub-tab';
                btn.dataset.tab = item.id;
                btn.textContent = item.label;
                btn.addEventListener('click', () => {
                    switchTab(item.id);
                    this.updateActiveTab(item.id);
                    this.closeMenu();
                });
                navbar.appendChild(btn);
            }
        });

        // Close dropdowns on outside click
        document.removeEventListener('click', this._closeHandler);
        this._closeHandler = () => this.closeDropdowns();
        document.addEventListener('click', this._closeHandler);

        this.updateActiveTab();
    },

    closeDropdowns() {
        document.querySelectorAll('#settingsHubTabs .hub-tab-dropdown.open')
            .forEach(d => d.classList.remove('open'));
    },

    // Update active visual state. Accepts an explicit tabId or derives from DOM.
    updateActiveTab(activeId) {
        if (!activeId) {
            const activePanel = document.querySelector('.tab-panel.active');
            if (!activePanel) return;
            activeId = activePanel.id.replace('tab-', '');
        }

        // Update mobile hamburger label
        const labelEl = document.getElementById('settingsCurrentTabLabel');

        // Reset all active states in the settings navbar
        document.querySelectorAll('#settingsHubTabs .hub-tab, #settingsHubTabs .hub-dropdown-item')
            .forEach(el => el.classList.remove('active'));

        // Find and activate the matching button
        const activeBtn = document.querySelector(
            `#settingsHubTabs .hub-tab[data-tab="${activeId}"], ` +
            `#settingsHubTabs .hub-dropdown-item[data-tab="${activeId}"]`
        );
        if (activeBtn) {
            activeBtn.classList.add('active');
            if (labelEl) labelEl.textContent = activeBtn.textContent.trim();
            // If it's a child, also highlight the parent split-label
            if (activeBtn.classList.contains('hub-dropdown-item')) {
                const parentDropdown = activeBtn.closest('.hub-tab-dropdown');
                if (parentDropdown) {
                    parentDropdown.querySelector('.hub-tab-label')?.classList.add('active');
                }
            }
        }
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
                <div class="menu-item-actions">
                    <button class="btn-edit-item" data-id="${item.id}" title="Edit label">✏️</button>
                </div>
            </div>
            <div class="menu-editor-children" data-parent="${item.id}">
                ${children.map(child => `
                    <div class="menu-editor-item menu-editor-child" data-id="${child.id}" draggable="true">
                        <div class="menu-item-header">
                            <span class="drag-handle">⋮⋮</span>
                            <span class="menu-item-icon">${child.icon}</span>
                            <span class="menu-item-label">${child.label.replace(child.icon, '').trim()}</span>
                            <div class="menu-item-actions">
                                <button class="btn-promote-item" data-id="${child.id}" title="Promote to top level">⬆️</button>
                            </div>
                        </div>
                    </div>
                `).join('')}
                <div class="drop-zone-child" data-parent="${item.id}">
                    <span>Drop here to nest as submenu item</span>
                </div>
            </div>
        `;

        // Wire buttons (no inline handlers — CSP-safe)
        div.querySelector('.btn-edit-item').addEventListener('click', () => this.editItem(item.id));
        div.querySelectorAll('.btn-promote-item').forEach(btn => {
            btn.addEventListener('click', () => this.promoteItem(btn.dataset.id));
        });

        return div;
    },

    editItem(id) {
        const item = this.currentMenu.find(m => m.id === id);
        if (!item) return;
        const newLabel = prompt('Enter new label (without emoji):', item.label.replace(item.icon, '').trim());
        if (newLabel !== null && newLabel.trim()) {
            item.label = item.icon + ' ' + newLabel.trim();
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
