// probes-menu.js — Split-dropdown navigation for the Probes group
// xarta-node Blueprints GUI
//
// Adapts the reference menu-system pattern (patterns/menu-system/) to
// the Probes group. Calls the existing switchTab() / switchGroup()
// infrastructure — does NOT manage its own tab panels.
//
// localStorage key: 'blueprintsProbesMenuConfig'
// (unique key — future group menus must use different keys)
//
// No inline event handlers — all event wiring via addEventListener.

'use strict';

const ProbesMenuConfig = {

    STORAGE_KEY: 'blueprintsProbesMenuConfig',

    _initialized: false,

    // ── Default menu structure ─────────────────────────────────
    // id     — must match the existing switchTab() tab IDs
    // parent — null = top-level; 'parentId' = child of that item
    // order  — sort order within level (0-based)
    // Note: items with children redirect to their first child on label click.

    defaultMenu: [
        { id: 'pfsense-dns',          label: '🔥 pfSense DNS',     icon: '🔥', pageLabel: 'pfSense DNS',       parent: null,             order: 0 },
        { id: 'proxmox-config',       label: '🖥 Proxmox Config',  icon: '🖥', pageLabel: 'Proxmox Config',    parent: null,             order: 1 },
        { id: 'vlans',                label: '🔀 VLANs',           icon: '🔀', pageLabel: 'VLANs',             parent: 'proxmox-config',  order: 0 },
        { id: 'ssh-targets',          label: '🎯 SSH Targets',     icon: '🎯', pageLabel: 'SSH Targets',       parent: 'proxmox-config',  order: 1 },
        { id: 'dockge-stacks',        label: '🐳 Dockge Stacks',   icon: '🐳', pageLabel: 'Dockge Stacks',     parent: 'proxmox-config',  order: 2 },
        { id: 'caddy-configs',        label: '🌐 Caddy Configs',   icon: '🌐', pageLabel: 'Caddy Configs',     parent: 'proxmox-config',  order: 3 },
        { id: 'bookmarks',            label: '🔖 Bookmarks',       icon: '🔖', pageLabel: 'Bookmarks',         parent: null,             order: 2 },
        { id: 'bookmarks-main',       label: '📋 Main',            icon: '📋', pageLabel: 'Bookmarks',         parent: 'bookmarks',       order: 0 },
        { id: 'bookmarks-history',    label: '📜 History',         icon: '📜', pageLabel: 'Visit History',     parent: 'bookmarks',       order: 1 },
        { id: 'bookmarks-embeddings', label: '🤖 Embeddings',      icon: '🤖', pageLabel: 'Embedding Config',  parent: 'bookmarks',       order: 2 },
        { id: 'bookmarks-setup',      label: '⚙ Setup',           icon: '⚙',  pageLabel: 'Setup & Import',    parent: 'bookmarks',       order: 3 },
        { id: 'probes-settings',      label: '☰',                 icon: '☰',  pageLabel: 'Navbar Layout',     parent: null,             order: 3 },
    ],

    currentMenu: [],
    _activeId: null,
    draggedItem: null,

    // ── Lifecycle ──────────────────────────────────────────────

    // Called by switchGroup('probes') each time the probes group becomes active.
    // Full setup on first call; subsequent calls just refresh active state.
    showGroup() {
        if (!this._initialized) {
            this.loadConfig();
            this.renderEditor();
            this.setupDragAndDrop();
            this._initialized = true;

            // Wire hamburger toggle
            const toggle = document.getElementById('probesMenuToggle');
            if (toggle) toggle.addEventListener('click', () => this.toggleMenu());

            // Wire save/reset buttons in navbar-layout panel
            const saveBtn = document.getElementById('probesMenuSaveButton');
            if (saveBtn) saveBtn.addEventListener('click', () => this.saveConfig(true));
            const resetBtn = document.getElementById('probesMenuResetButton');
            if (resetBtn) resetBtn.addEventListener('click', () => this.resetConfig());
        }
        // Always refresh active state when the group re-activates (also renders navbar)
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
                    } else if (existing.pageLabel === undefined) {
                        existing.pageLabel = def.pageLabel;
                    }
                });
            } catch (e) {
                console.error('[ProbesMenuConfig] Failed to parse saved config:', e);
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
        if (confirm('Reset probes menu to default layout?')) {
            localStorage.removeItem(this.STORAGE_KEY);
            this.currentMenu = JSON.parse(JSON.stringify(this.defaultMenu));
            this.saveConfig(false);
            this.renderEditor();
            this.setupDragAndDrop();
        }
    },

    showSaveNotification() {
        const notif = document.getElementById('probesMenuSaveNotification');
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
        const topItems = document.querySelectorAll('#probesMenuEditorList > .menu-editor-item');
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
        const toggle = document.getElementById('probesMenuToggle');
        const tabs   = document.getElementById('probesHubTabs');
        if (!toggle || !tabs) return;
        const open = tabs.classList.toggle('open');
        toggle.classList.toggle('open', open);
    },

    closeMenu() {
        const toggle = document.getElementById('probesMenuToggle');
        const tabs   = document.getElementById('probesHubTabs');
        if (toggle) toggle.classList.remove('open');
        if (tabs)   tabs.classList.remove('open');
    },

    // ── Navbar rendering ───────────────────────────────────────

    renderNavbar(activeId) {
        if (activeId === undefined) activeId = this._activeId;
        const navbar = document.getElementById('probesHubTabs');
        if (!navbar) return;
        navbar.innerHTML = '';

        this.getTopLevelItems().forEach(item => {
            const children = this.getChildren(item.id);
            const allInGroup = [item, ...children];
            // Which member of this group (if any) is the currently active tab?
            const activeMember = activeId ? allInGroup.find(m => m.id === activeId) : null;
            const isGroupActive = !!activeMember;

            if (children.length > 0) {
                // Group with children: split-button + dropdown
                const labelText = isGroupActive
                    ? (activeMember.pageLabel || activeMember.label)
                    : item.label;
                // When active: show all group members EXCEPT the active one in dropdown.
                // When inactive: show children only (parent is the labelled button as usual).
                const dropdownItems = isGroupActive
                    ? allInGroup.filter(m => m.id !== activeMember.id)
                    : children;

                const dropdown = document.createElement('div');
                dropdown.className = 'hub-tab-dropdown';
                dropdown.innerHTML = `
                    <div class="hub-tab-split">
                        <button class="hub-tab hub-tab-label${isGroupActive ? ' active' : ''}" data-tab="${item.id}">${labelText}</button>
                        <button class="hub-tab-caret" aria-label="Toggle submenu">▼</button>
                    </div>
                    <div class="hub-dropdown-menu">
                        ${dropdownItems.map(c => `<button class="hub-dropdown-item" data-tab="${c.id}">${c.label}</button>`).join('')}
                    </div>
                `;

                // Label click: if group active → re-navigate to active member;
                // else → navigate to parent's own tab or first child.
                dropdown.querySelector('.hub-tab-label').addEventListener('click', (e) => {
                    e.stopPropagation();
                    const targetId = isGroupActive
                        ? activeMember.id
                        : (document.getElementById('tab-' + item.id) ? item.id : children[0]?.id);
                    if (targetId) {
                        switchTab(targetId);
                        this.updateActiveTab(targetId);
                        this.closeMenu();
                        this.closeDropdowns();
                    }
                });

                // Caret → toggle submenu open/close
                dropdown.querySelector('.hub-tab-caret').addEventListener('click', (e) => {
                    e.stopPropagation();
                    const wasOpen = dropdown.classList.contains('open');
                    this.closeDropdowns();
                    if (!wasOpen) dropdown.classList.add('open');
                });

                // Dropdown item clicks → navigate (resolve missing panels to first child)
                dropdown.querySelectorAll('.hub-dropdown-item').forEach(btn => {
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

                navbar.appendChild(dropdown);

            } else {
                // Standalone tab button (no children)
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

        // Close dropdowns on outside click
        document.removeEventListener('click', this._closeHandler);
        this._closeHandler = () => this.closeDropdowns();
        document.addEventListener('click', this._closeHandler);
    },

    closeDropdowns() {
        document.querySelectorAll('#probesHubTabs .hub-tab-dropdown.open')
            .forEach(d => d.classList.remove('open'));
    },

    // Update active visual state. Accepts an explicit tabId or derives from DOM.
    updateActiveTab(activeId) {
        if (!activeId) {
            const activePanel = document.querySelector('.tab-panel.active');
            if (activePanel) activeId = activePanel.id.replace('tab-', '');
        }
        if (activeId) this._activeId = activeId;

        // Update mobile hamburger label
        const labelEl = document.getElementById('probesCurrentTabLabel');
        if (labelEl && activeId) {
            const item = this.currentMenu.find(m => m.id === activeId);
            if (item) labelEl.textContent = item.pageLabel || item.label;
        }

        // Re-render navbar with active state baked in
        this.renderNavbar(activeId || this._activeId);
    },

    // ── Editor rendering ───────────────────────────────────────

    renderEditor() {
        const container = document.getElementById('probesMenuEditorList');
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
                ${children.map(child => `
                    <div class="menu-editor-item menu-editor-child" data-id="${child.id}" draggable="true">
                        <div class="menu-item-header">
                            <span class="drag-handle">⋮⋮</span>
                            <span class="menu-item-icon">${child.icon}</span>
                            <span class="menu-item-label">${child.label.replace(child.icon, '').trim()}</span>
                            <span class="menu-item-page-label" title="Page label">→ ${child.pageLabel || '—'}</span>
                            <div class="menu-item-actions">
                                <button class="btn-edit-page-label" data-id="${child.id}" title="Edit page label">🏷️</button>
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
        const container = document.getElementById('probesMenuEditorList');
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
                    const list  = document.getElementById('probesMenuEditorList');
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
