# gui-embed

Embeddable Blueprints node-selector web component.

Drop the node-selector widget into any page in your homelab — dashboards,
internal tools, Homarr, Heimdall, custom HTML — with three lines of HTML.

The component auto-discovers all Blueprints nodes from a single entry point,
health-checks them every 10 s, and fails over automatically.

---

## Files

| File | Purpose |
|---|---|
| `blueprints-node-selector.js` | The web component (no build step, no dependencies) |
| `blueprints-node-selector.css` | Component styles |
| `blueprints-auth.js` | TOTP auth helper — defines `window.apiFetch`; required on any site that doesn't define its own |
| `embed.html` | Standalone demo page |
| `config.example.js` | Config template — copy to `config.js` and edit |
| `config.js` | Your site-specific config (gitignored) |

---

## Embedding in an existing page

### 1. Paste into your HTML

```html
<!-- Set the API base URL before loading the component -->
<script>
  window.BLUEPRINTS_API_BASE = 'http://<your-node-ip>:8080';
</script>

<!-- Load assets directly from the node (always up to date) -->
<link  rel="stylesheet" href="http://<your-node-ip>:8080/ui/embed/blueprints-node-selector.css" />
<script src="http://<your-node-ip>:8080/ui/embed/blueprints-auth.js"></script>
<script src="http://<your-node-ip>:8080/ui/embed/blueprints-node-selector.js"></script>

<!-- Place the element wherever you want the selector to appear -->
<blueprints-node-selector></blueprints-node-selector>
```

`blueprints-auth.js` handles TOTP authentication. On any site that doesn't already define `window.apiFetch`, it installs one that reads the API secret from `localStorage` (key `blueprints_api_secret`) and derives a time-based token for each request. On a 401 it prompts for the secret once and caches it. The main Blueprints GUI defines its own `apiFetch` — if both scripts are present, `blueprints-auth.js` detects this and does nothing, so there is no conflict.

That's it. The component discovers all peer nodes from the API and keeps
itself up to date — no extra configuration needed unless you want seed nodes.

### 2. Optional: seed nodes

Seed nodes are merged with API-discovered ones and serve as a fallback when
the API base is temporarily unreachable:

```html
<script>
  window.BLUEPRINTS_API_BASE   = 'http://<node-a-ip>:8080';
  window.BLUEPRINTS_SEED_NODES = [
    { id: 'node-b', name: 'Node B', url: 'http://<node-b-ip>:8080' },
  ];
</script>
```

### 3. Optional: selector action buttons

You can add icon buttons next to the dropdown and control which side they
appear on, which buttons show, and how paging works.

```html
<script>
  window.BLUEPRINTS_SELECTOR_BUTTONS = {
    enabledButtons: ['ui', 'fallback-ui', 'database-tables', 'database-diagram', 'paging-button'],
    side: 'left',
    pageSize: 4,
    nodeSwitchPath: 'current'
  };
</script>
```

Supported button keys:
- `ui`
- `fallback-ui`
- `database-tables`
- `database-diagram`
- `paging-button` (cycles to next button page)

---

## Using embed.html locally

`embed.html` is a self-contained demo page. To use it:

1. Copy `config.example.js` to `config.js`
2. Set `window.BLUEPRINTS_API_BASE` to any live node's URL
3. Serve the folder from any web server:
   ```bash
   python3 -m http.server 8000
   # then open http://localhost:8000/embed.html
   ```
   (Opening `embed.html` as a `file://` URL will work for the widget itself,
   but the API discovery fetch calls require a server origin.)

---

## CORS

The Blueprints API allows cross-origin requests from any origin
(`Access-Control-Allow-Origin: *`), so embedding from a different host,
port, or subdomain **requires no CORS configuration**. The API is designed
for isolated tailnet/LAN use — keep nodes off the public internet.

---

## How assets are served by the node

`setup-blueprints.sh` creates a symlink:

```
<gui-dir>/embed  →  <repo>/gui-embed/
```

This means the files in this folder are served by the running Blueprints node
at `/ui/embed/...` — so the embed assets always reflect the current state of
this repo without any extra copy step.

The Blueprints GUI (`index.html`) also references the component via this path,
so there is only one copy of the source files.

Shared database UI pages now live in the public folder `gui-db/` and are served
at:
- `/ui/db/...` (via `.xarta/gui/db` symlink)
- `/fallback-ui/db/...` (via `gui-fallback/db` symlink)

This keeps database table/diagram pages single-sourced in the public repo while
allowing `/ui` and `/fallback-ui` main pages to diverge independently.
