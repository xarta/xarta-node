# Blueprints Bookmarks — Edge Extension

A Chromium Manifest V3 extension for saving, searching, and passively recording bookmarks via the Blueprints API.

## Features

- **One-click save** — save the current tab to your Blueprints bookmark store
- **Search** — hybrid keyword + vector search via SeekDB (powered by the Blueprints API)
- **Context menu** — right-click any page or link to save it
- **Visit recorder** — optional passive mode that logs every page visited

## Files

```
extension/
├── manifest.json           Manifest V3 declaration
├── popup.html / popup.js   Extension popup (save + search)
├── service-worker.js       Background worker (context menu + visit recorder)
├── options.html / options.js  Settings page (API URL, secret, recorder config)
├── shared/
│   └── api.js              TOTP auth wrapper (matches blueprints GUI api.js)
└── README.md               This file
```

## Installation (developer mode)

1. Open Edge → navigate to `edge://extensions/`
2. Enable **Developer mode** (toggle top-right)
3. Click **Load unpacked**
4. Select this `extension/` folder
5. The extension appears in your toolbar

## Configuration

After installing, click **Options** (or right-click the extension icon → Options):

- **API URL** — the HTTPS URL of your Blueprints node (e.g. `https://your-node.example.com`)
  - Use your LAN hostname when on the LAN, Tailscale hostname when remote
- **API Secret** — paste your `BLUEPRINTS_API_SECRET` from `.env` on the node
- Click **Test connection** to verify

## Authentication

The extension uses the same TOTP scheme as the Blueprints GUI:
- `HMAC-SHA256(secret_hex, floor(unix_time / 5))` — 5-second windows
- The raw secret is stored in `chrome.storage.local` (encrypted at rest by the browser)
- Only the derived token is ever sent to the API, never the raw secret

## Visit recorder

When enabled in Options:
- Every page you navigate to is recorded as a visit via `POST /api/v1/bookmarks/visits`
- Incognito windows are never recorded (extension does not run in incognito mode)
- Skip prefixes (configurable) filter out local/internal URLs

## Icons

Place PNG icons at these paths (required for production use):
- `icons/icon-16.png` (16×16)
- `icons/icon-48.png` (48×48)
- `icons/icon-128.png` (128×128)

Without icons the extension loads correctly in developer mode with a default placeholder icon.

To generate icons from SVG, any of these tools work:
```bash
inkscape icon.svg --export-png=icons/icon-128.png --export-width=128
# or
convert -background none icon.svg -resize 128x128 icons/icon-128.png
```
