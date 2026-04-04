---
name: blueprints-gui
description: Work on the Blueprints GUI — the HTML/CSS/JS dashboard and node-selector embed served by each xarta-node. Use when the user wants to edit, improve, or debug the Blueprints web interface, node selector component, or embed widget. Also use when distributing GUI updates across the fleet via sync_git_inner.
---

# blueprints-gui skill

Work on the Blueprints GUI — the HTML/CSS/JS dashboard served at `/ui` on every
xarta-node.  Use when editing visual behaviour, tab content, styling, or any
JavaScript feature of the GUI.

Prefer shared GUI components over page-local duplication. If two pages need the same chooser,
picker, modal body, or editor behavior, extract the shared part into one named module and keep
page-specific behavior in small adapters or callbacks.

---

## File layout

All source lives in **`gui-fallback/`** (public outer repo). This is the primary Blueprints dashboard GUI — all feature work, layout, and CSS changes start here.

> ⚠️ The private inner repo (`.xarta/gui/`) contains an independent private GUI that diverged from `gui-fallback/` in 2026. **Do NOT copy or sync between `gui-fallback/` and `.xarta/gui/`** — they are maintained independently. The live app serves the private GUI from `BLUEPRINTS_GUI_DIR`; file changes there take effect immediately without a restart.

---

## Required reading for shared table and bucket work

Before changing shared table behavior, read the relevant node-local docs first:

- `/xarta-node/.lone-wolf/docs/web-design/TABLE-MIGRATION.md` for moving a page-local table onto the shared frontend table layer
- `/xarta-node/.lone-wolf/docs/web-design/TABLE-BUCKET-MIGRATION.md` for moving an already-shared table onto `table_layout_catalog` and `table_layouts`
- `/xarta-node/.lone-wolf/docs/web-design/COLUMN-SIZE-INDEXING.md` for the canonical `RRUUTTBB` bucket contract

If the target is the Nav Items or Form Controls system, read these too before changing storage or layout behavior:

- `/xarta-node/.lone-wolf/docs/web-design/NAV-ITEMS.md`
- `/xarta-node/.lone-wolf/docs/web-design/FORM-CONTROLS.md`

For shared modal, chooser, or reusable admin-surface work, also read:

- `/xarta-node/.lone-wolf/docs/web-design/MODAL.md`
- `/xarta-node/.lone-wolf/docs/web-design/COMPONENT-LIBRARY.md`

For ultrawide sidecar debug panel behavior and extending debug scenarios:

- `/xarta-node/.lone-wolf/docs/web-design/ULTRA-WIDE-DEBUG-PANELS.md`

If working on Dockge or Caddy probe tabs, also read:

- `/xarta-node/.lone-wolf/docs/dockge/README.md`
- `/xarta-node/.lone-wolf/docs/dockge/OPEN-NOTEBOOK.md`
- `/xarta-node/.lone-wolf/docs/caddy/CADDY.md`

These node-local docs may be unavailable on some LXCs/workspaces, especially in public-scope skill contexts that cannot see node-local/private paths. If missing, continue using this skill and the codebase as the authoritative source.

---

## Load order (index.html)

CSS (in `<head>`, in this order):
```
css/tokens.css              ← CSS custom properties + global reset
css/layout-nav.css          ← header, group-tabs, node-meta, table-nav, tab-panel
css/tables.css              ← tables, tags, badges, status colours, ip-chip, kind-badge
css/components.css          ← sync panel, toolbar, buttons, inputs, spinner, backup styles
css/hub-menu.css            ← hub-menu split-dropdown nav bar (shared across all groups)
css/probes-menu.css         ← Probes group nav ID overrides
css/settings-menu.css       ← Settings group nav ID overrides
css/synthesis-menu.css      ← Synthesis group nav ID overrides
css/hub-controls.css        ← form controls (inputs, selects) + HubSelect popup
css/responsive-header.css   ← two-state responsive header + .header-inner compact rules
css/body-shade.css          ← body shade drag handle + fill-table sizing
css/hub-modal.css           ← hub-modal <dialog> component
```

JavaScript (`<script src=...>` at end of `<body>`, **must stay in this order**):
```
js/utils.js                  ← esc() HTML-escape utility (used by everyone)
js/api.js                    ← TOTP auth, apiFetch(), openApiKeyModal(), saveApiKey()
js/state.js                  ← global mutable state (_services, _machines, …)
js/connectivity.js           ← offline connectivity diagnostic modal + helpers
js/health.js                 ← loadHealth(), updateKeyBadge(), openIntegrityModal(), lookupHostParent()
js/sync.js                   ← loadSyncStatus()
js/backups.js                ← loadBackups(), createBackup(), confirmRestore(), submitRestore()
js/synthesis/manual-links.js ← Manual Links tab (rendered view + table)
js/synthesis/services.js     ← loadServices(), renderServices(), openAddModal(), submitAddService()
js/synthesis/machines.js     ← loadMachines(), renderMachines()
js/sound-manager.js          ← SoundManager IIFE (tab-switch audio cues)
js/form-control-manager.js   ← FormControlManager IIFE (configurable key-value form controls)
js/hub-modal.js              ← HubModal IIFE — must load before hub-menu.js
js/hub-menu.js               ← createHubMenu() factory — shared nav engine
js/hieroglyphs.js            ← HIEROGLYPHS constant — SVG data-URIs for nav icons
js/synthesis/synthesis-menu.js  ← Synthesis group nav wrapper
js/settings/nodes.js         ← Fleet Nodes tab, fleetUpdate(), nodeRestart/GiPull/PCT
js/settings/keys.js          ← SSH key management + AES-256-GCM key store + deploy UI
js/settings/certs.js         ← Certificates tab
js/settings/docs-history.js  ← Docs history/version panel
js/settings/docs.js          ← Docs editor tab
js/settings/docs-images.js   ← Doc Images tab
js/settings/self-diag.js     ← Self Diagnostic tab (_DIAG_ENDPOINTS, runSelfDiag)
js/settings/settings.js      ← App Config key-value tab
js/settings/pve-hosts.js     ← PVE Hosts tab
js/settings/arp-manual.js    ← Manual ARP tab
js/settings/ai-providers.js  ← AI Providers tab
js/settings/nav-items.js     ← Nav Items tab
js/settings/form-controls.js ← Form Controls tab
js/probes/pfsense.js         ← pfSense DNS tab
js/probes/proxmox-config.js  ← Proxmox Config tab (probe, nets, find-IPs)
js/probes/vlans.js           ← VLANs tab
js/probes/ssh-targets.js     ← SSH Targets tab
js/probes/dockge.js          ← Dockge Stacks tab
js/probes/caddy.js           ← Caddy Configs tab
js/table-pager.js            ← TablePager IIFE — shared pagination engine
js/probes/bookmarks.js       ← Bookmarks/Visits tabs + browser extension feature
js/probes/probes-menu.js     ← Probes group nav wrapper
js/settings/settings-menu.js ← Settings group nav wrapper
js/hub-select.js             ← HubSelect IIFE — custom dropdown with smart positioning
js/responsive-layout.js      ← ResponsiveLayout IIFE — compact/full header observer
js/body-shade.js             ← body-shade drag + fill-table sizing logic
js/app.js                    ← switchGroup(), switchTab(), DOMContentLoaded bootstrap ← MUST BE LAST
```

Embed component (after all app JS):
```
embed/blueprints-node-selector.css   (served via symlink)
embed/blueprints-node-selector.js    (served via symlink)
```

Current selector contract to keep in mind before editing it:
- `pageSize` defines a fixed action-slot region, not a hard-coded three-button worldview
- short pages are left-padded with placeholder circles so the right edge stays stable
- when enabled, the last two positions stay fixed as scarab paging button then origin button
- if `/xarta-node/.lone-wolf/docs/web-design/EMBEDDED-GUI.md` exists on the current node, read it before changing embed behavior; otherwise inspect the current selector source directly

---

## Navigation groups → tabs → JS file quick-reference

### Synthesis group
| Tab | HTML section id | JS file |
|-----|----------------|---------|
| Services | `#tab-services` | `js/synthesis/services.js` |
| Machines | `#tab-machines` | `js/synthesis/machines.js` |

### Probes group
| Tab | HTML section id | JS file |
|-----|----------------|---------|
| pfSense DNS | `#tab-pfsense-dns` | `js/probes/pfsense.js` |
| Proxmox Config | `#tab-proxmox-config` | `js/probes/proxmox-config.js` |
| VLANs | `#tab-vlans` | `js/probes/vlans.js` |
| SSH Targets | `#tab-ssh-targets` | `js/probes/ssh-targets.js` |
| Dockge Stacks | `#tab-dockge-stacks` | `js/probes/dockge.js` |
| Caddy Configs | `#tab-caddy-configs` | `js/probes/caddy.js` |

### Settings group
| Tab | HTML section id | JS file |
|-----|----------------|---------|
| Settings | `#tab-settings` | `js/settings/settings.js` |
| PVE Hosts | `#tab-pve-hosts` | `js/settings/pve-hosts.js` |
| Manual ARP | `#tab-arp-manual` | `js/settings/arp-manual.js` |
| Nodes | `#tab-nodes` | `js/settings/nodes.js` |
| Keys | `#tab-keys` | `js/settings/keys.js` |
| Basic Assumptions | `#tab-assumptions` | `js/settings/assumptions.js` |
| Self Diagnostic | `#tab-self-diag` | `js/settings/self-diag.js` |

---

## CSS topic index

| I want to change… | File |
|---|---|
| Colours, fonts, radius, CSS variables | `css/tokens.css` |
| Overall page layout, `<header>`, `<main>` spacings | `css/layout-nav.css` |
| Node meta badges (Gen, OK, FAILED, key count) | `css/layout-nav.css` — `.badge*` |
| Tab panel show/hide | `css/layout-nav.css` — `.tab-panel` |
| Data table headers, rows, hover | `css/tables.css` |
| Coloured tags (`tag`), link badges, name links | `css/tables.css` |
| Status colours (deployed / wip / planned) | `css/tables.css` |
| IP chips, machine kind badges | `css/tables.css` |
| Toolbar, filter inputs | `css/components.css` |
| Primary / secondary buttons | `css/components.css` |
| Loading spinner | `css/components.css` |
| Sync status panel grid | `css/components.css` |
| Backups table, restore / force-restore buttons | `css/components.css` |
| Warning / error / success box colours | `css/components.css` |
| Split-dropdown nav bar (all groups) | `css/hub-menu.css` |
| Probes group nav overrides | `css/probes-menu.css` |
| Settings group nav overrides | `css/settings-menu.css` |
| Synthesis group nav overrides | `css/synthesis-menu.css` |
| Form control inputs, HubSelect dropdown popup | `css/hub-controls.css` |
| Responsive two-state header (compact / full) | `css/responsive-header.css` |
| Body shade drag handle, fill-table sizing | `css/body-shade.css` |
| Modal dialogs (`<dialog class="hub-modal">`) | `css/hub-modal.css` |

---

## JS behaviour index

| Behaviour | File | Key function(s) |
|---|---|---|
| API key prompt & localStorage | `js/api.js` | `openApiKeyModal`, `saveApiKey` |
| TOTP token generation | `js/api.js` | `_computeApiToken` |
| Every API call | `js/api.js` | `apiFetch` |
| Offline diagnostic modal | `js/connectivity.js` | `showConnectivityDiagnostic` |
| Peer reachability check | `js/connectivity.js` | `_checkPeerNodes` |
| Internet reachability check | `js/connectivity.js` | `_checkInternet` |
| Header: Gen, integrity badge, peers | `js/health.js` | `loadHealth`, `updateKeyBadge` |
| Integrity FAILED modal | `js/health.js` | `openIntegrityModal` |
| Host parent lookup | `js/health.js` | `lookupHostParent` |
| Switching top group (Synthesis/Probes/Settings) | `js/app.js` | `switchGroup` |
| Switching inner tabs | `js/app.js` | `switchTab` |
| Page bootstrap (on load) | `js/app.js` | `DOMContentLoaded` handler |
| Sync status panel | `js/sync.js` | `loadSyncStatus` |
| Backup list & create | `js/backups.js` | `loadBackups`, `createBackup` |
| Restore backup | `js/backups.js` | `confirmRestore`, `submitRestore` |
| Services table | `js/synthesis/services.js` | `loadServices`, `renderServices` |
| Add service modal | `js/synthesis/services.js` | `openAddModal`, `submitAddService` |
| Machines table | `js/synthesis/machines.js` | `loadMachines`, `renderMachines` |
| Fleet Nodes table + fleet update | `js/settings/nodes.js` | `loadNodes`, `fleetUpdate` |
| Node restart / git pull / purge queue | `js/settings/nodes.js` | `nodeRestart`, `nodeGitPull`, `nodePurgeQueue` |
| LXC start/stop via PCT | `js/settings/nodes.js` | `nodePct`, `enrichNodePctStatus` |
| SSH keys list & status table | `js/settings/keys.js` | `loadKeys`, `renderKeysTable` |
| Import key bundle | `js/settings/keys.js` | `parseKeyBundle`, `importSelectedKeys` |
| Encrypted key store (AES-256-GCM) | `js/settings/keys.js` | `saveToStore`, `loadFromStore`, `openEncrypted` |
| Web Crypto key derive/encrypt/decrypt | `js/settings/keys.js` | `_ksDeriveKey`, `_ksEncrypt`, `_ksDecrypt` |
| Basic Assumptions editor + markdown preview | `js/settings/assumptions.js` | `loadAssumptions`, `assumptionsSave`, `_mdToHtml` |
| Self Diagnostic run | `js/settings/self-diag.js` | `runSelfDiag` |
| Settings key-value store | `js/settings/settings.js` | `loadSettings`, `saveCidr` |
| PVE Hosts scan | `js/settings/pve-hosts.js` | `scanPveHosts`, `loadPveHosts` |
| Manual ARP entries | `js/settings/arp-manual.js` | `loadArpManual`, `addArpManualEntry` |
| pfSense DNS probe | `js/probes/pfsense.js` | `probePfSense`, `pingSweep` |
| Proxmox Config full probe | `js/probes/proxmox-config.js` | `fullProbeProxmox`, `probeProxmoxConfig` |
| Find IPs (ARP / PVE / QEMU / pfSense) | `js/probes/proxmox-config.js` | `findIpsByArp`, `findIpsViaPve`, `findIpsViaQemuAgent` |
| Proxmox net NIC expand/collapse | `js/probes/proxmox-config.js` | `toggleNets`, `setAllNets` |
| VLAN CIDR edit | `js/probes/vlans.js` | `editVlan` |
| SSH Targets rebuild | `js/probes/ssh-targets.js` | `rebuildSshTargets` |
| Dockge Stacks probe | `js/probes/dockge.js` | `probeDockgeStacks`, `renderDockgeStacks` |
| Caddy Configs probe | `js/probes/caddy.js` | `probeCaddyConfigs` |

---

## HTML modals index

All modals use `<dialog>` elements in `index.html`. Those with `class="hub-modal"` use `HubModal.open()`; legacy dialogs use `.showModal()` directly.

| Modal | HTML `id` | Opened by |
|---|---|---|
| Add/edit bookmark | `#bm-modal` | `_bmOpenModal()` in bookmarks.js |
| Add/edit manual link | `#ml-modal` | `mlOpenModal()` in manual-links.js |
| Add service | `#add-modal` | `openAddModal()` in services.js |
| Integrity FAILED | `#integrity-modal` | `openIntegrityModal()` in health.js |
| Edit VLAN CIDR | `#vlan-modal` | `editVlan()` in vlans.js |
| Upload certificate | `#certs-upload-modal` | `openCertsUploadModal()` in certs.js |
| Edit Manual ARP entry | `#arp-manual-edit-modal` | `openArpEdit()` in arp-manual.js |
| Edit PVE host | `#pve-host-edit-modal` | `openPveHostEdit()` in pve-hosts.js |
| Add/edit setting | `#setting-modal` | `openAddSettingModal()` / `editSetting()` in settings.js |
| Add doc | `#add-doc-modal` | `openAddDocModal()` in docs.js |
| View doc | `#docs-modal` | `openDocsModal()` in docs.js |
| Delete doc confirm | `#docs-delete-modal` | `openDocsDeleteModal()` in docs.js |
| Restore confirmation | `#restore-modal` | `confirmRestore()` in backups.js |
| Key info | `#key-info-modal` | `openKeyInfo()` in keys.js |
| Connectivity diagnostic | `#diag-modal` | `showConnectivityDiagnostic()` in connectivity.js |
| API key | `#api-key-modal` | `openApiKeyModal()` in api.js |
| Bookmark excluded tags | `#bm-excl-tag-modal` | `_bmOpenExclTagModal()` in bookmarks.js |
| Bookmark column visibility | `#bm-cols-modal` | `_bmOpenColsModal()` in bookmarks.js |
| Visit column visibility | `#vis-cols-modal` | `_visOpenColsModal()` in bookmarks.js |
| Score explain | `#bm-score-modal` | `_bmOpenScoreModal()` in bookmarks.js |
| Score detail (drill-down) | `#bm-score-detail-modal` | `_bmOpenScoreDetailModal()` in bookmarks.js |
| Sort explain | `#bm-sort-explain-modal` | `_bmOpenSortExplainModal()` in bookmarks.js |

---

## Global state variables (`js/state.js`)

```
_services        loaded services array
_machines        loaded machines array
_nodes           loaded nodes array
_settings        loaded settings array
_nodeName        display name of this node (from /health)
_selfNodeId      node_id from /health
_activeGroup     current top-level nav group ('synthesis'|'probes'|'settings')
_pfsenseDns      pfSense DNS records
_proxmoxConfig   Proxmox config rows
_proxmoxNetsMap  config_id → [net, …] map
_dockgeStacks    Dockge stack rows
_dockgeServicesMap  stack_id → [service, …] map
_caddyConfigs    Caddy config rows
_pveHosts        PVE hosts rows
_vlans           VLAN rows
_arpManual       Manual ARP rows
_sshTargets      SSH target rows
_keys            SSH key status rows
_parsedBundle    Parsed key entries from import textarea
_pctPollInterval setInterval handle for PCT status polling
```

Constants defined in `js/connectivity.js` (available globally since loaded first):
```
_LS_DIAG_NODE     localStorage key: this node's node_id
_LS_DIAG_HOST     localStorage key: cached parent host name
_LS_DIAG_HOST_TS  localStorage key: timestamp of cached parent
_LS_DIAG_NODES    localStorage key: cached peer node list
_HOST_TTL_MS      TTL for cached host parent (1 hour)
```

---

## Embed component

Node selector lives in `gui-embed/` (separate subdirectory):
- `gui-embed/blueprints-auth.js`           — TOTP auth helper for embed
- `gui-embed/blueprints-node-selector.js`  — `<blueprints-node-selector>` web component
- `gui-embed/blueprints-node-selector.css` — styles for the selector
- `gui-embed/config.example.js`            — configuration example

Symlinked into the GUI serve directory via `setup-blueprints.sh`.

---

## Database view pages

Separate HTML pages for DB schema viewing:
- `gui-fallback/database-diagram.html` → uses `gui-db/database-diagram.css` + `gui-db/database-diagram.js`
- `gui-fallback/database-tables.html`  → uses `gui-db/database-tables.css`  + `gui-db/database-tables.js`

These are standalone pages, not part of the main SPA.
