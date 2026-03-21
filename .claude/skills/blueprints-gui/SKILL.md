---
name: blueprints-gui
description: Work on the Blueprints GUI — the HTML/CSS/JS dashboard and node-selector embed served by each xarta-node. Use when the user wants to edit, improve, or debug the Blueprints web interface, node selector component, or embed widget. Also use when distributing GUI updates across the fleet via sync_git_inner.
---

# blueprints-gui skill

Work on the Blueprints GUI — the HTML/CSS/JS dashboard served at `/ui` on every
xarta-node.  Use when editing visual behaviour, tab content, styling, or any
JavaScript feature of the GUI.

---

## File layout

All source lives in **`gui-fallback/`** (public outer repo).  The private inner
repo (`/root/xarta-node/.xarta/gui/`) is a mirror.  Sync direction is always
**fallback → private**:

```bash
# After any changes in gui-fallback/:
cp gui-fallback/index.html        .xarta/gui/index.html
cp -r gui-fallback/css/           .xarta/gui/
cp -r gui-fallback/js/            .xarta/gui/

# Or all at once (safest):
rsync -a --delete \
  gui-fallback/css/ gui-fallback/js/ \
  gui-fallback/index.html \
  gui-fallback/database-diagram.html gui-fallback/database-tables.html \
  .xarta/gui/
```

The live app (`BLUEPRINTS_GUI_DIR=/root/xarta-node/.xarta/gui`) serves the
private copy immediately — no restart needed after file changes.

---

## Load order (index.html)

CSS (in `<head>`):
```
css/tokens.css           ← CSS custom properties + global reset
css/layout-nav.css       ← header, group-tabs, node-meta, table-nav, tab-panel
css/tables.css           ← tables, tags, badges, status colours, ip-chip, kind-badge
css/components.css       ← sync panel, toolbar, buttons, inputs, spinner, backup styles
```

JavaScript (`<script src=...>` at end of `<body>`, **must stay in this order**):
```
js/utils.js              ← esc() HTML-escape utility (used by everyone)
js/api.js                ← TOTP auth, apiFetch(), openApiKeyModal(), saveApiKey()
js/state.js              ← global mutable state (_services, _machines, …)
js/connectivity.js       ← offline connectivity diagnostic modal + helpers
js/health.js             ← loadHealth(), updateKeyBadge(), openIntegrityModal(), lookupHostParent()
js/sync.js               ← loadSyncStatus()
js/backups.js            ← loadBackups(), createBackup(), confirmRestore(), submitRestore()
js/synthesis/services.js ← loadServices(), renderServices(), openAddModal(), submitAddService()
js/synthesis/machines.js ← loadMachines(), renderMachines()
js/settings/nodes.js     ← loadNodes(), renderNodes(), fleetUpdate(), nodeRestart/GiPull/PCT etc.
js/settings/keys.js      ← all key management + key-store AES-256-GCM crypto + deploy UI
js/settings/assumptions.js  ← Basic Assumptions tab + markdown renderer
js/settings/self-diag.js    ← Self Diagnostic tab (_DIAG_ENDPOINTS, runSelfDiag)
js/settings/settings.js     ← Settings key-value tab
js/settings/pve-hosts.js    ← PVE Hosts tab
js/settings/arp-manual.js   ← Manual ARP tab
js/probes/pfsense.js         ← pfSense DNS tab
js/probes/proxmox-config.js  ← Proxmox Config tab (probe, nets, find-IPs)
js/probes/vlans.js           ← VLANs tab
js/probes/ssh-targets.js     ← SSH Targets tab
js/probes/dockge.js          ← Dockge Stacks tab
js/probes/caddy.js           ← Caddy Configs tab
js/app.js                ← switchGroup(), switchTab(), DOMContentLoaded bootstrap ← MUST BE LAST
```

Embed component (after all app JS):
```
embed/blueprints-node-selector.css   (served via symlink)
embed/blueprints-node-selector.js    (served via symlink)
```

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
| Top-level nav (Synthesis / Probes / Settings buttons) | `css/layout-nav.css` — `.group-tab` |
| Node meta badges (Gen, OK, FAILED, key count) | `css/layout-nav.css` — `.badge*` |
| Secondary tab bar (Services, Machines … tabs) | `css/layout-nav.css` — `.table-nav` |
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

All modals use `<dialog>` elements in `index.html`:

| Modal | HTML `id` | Opened by |
|---|---|---|
| Add service | `#add-modal` | `openAddModal()` in services.js |
| Integrity FAILED | `#integrity-modal` | `openIntegrityModal()` in health.js |
| Add/edit setting | `#setting-modal` | `openAddSettingModal()` / `editSetting()` in settings.js |
| Restore confirmation | `#restore-modal` | `confirmRestore()` in backups.js |
| Key info | `#key-info-modal` | `openKeyInfo()` in keys.js |
| Connectivity diagnostic | `#diag-modal` | `showConnectivityDiagnostic()` in connectivity.js |
| API key | `#api-key-modal` | `openApiKeyModal()` in api.js |

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
