/* options.js — Blueprints Bookmarks Extension settings page */

const DEFAULT_SKIP = [
  'localhost',
  '127.',
  '192.168.',
  '10.',
  '172.',
  'chrome://',
  'edge://',
  'about:',
  'file://',
].join('\n');

async function _load() {
  return new Promise(resolve => {
    chrome.storage.local.get(
      ['blueprints_api_url', 'blueprints_api_secret', 'recorder_enabled', 'recorder_skip'],
      resolve
    );
  });
}

function _statusEl(id) { return document.getElementById(id); }
function _show(id, msg, cls) {
  const el = _statusEl(id);
  el.textContent = msg;
  el.className = `status ${cls}`;
}

document.addEventListener('DOMContentLoaded', async () => {
  const cfg = await _load();
  document.getElementById('api-url').value           = cfg.blueprints_api_url    || '';
  document.getElementById('api-secret').value        = cfg.blueprints_api_secret  || '';
  document.getElementById('recorder-enabled').checked = !!cfg.recorder_enabled;
  document.getElementById('recorder-skip').value     =
    cfg.recorder_skip ? cfg.recorder_skip.join('\n') : DEFAULT_SKIP;

  document.getElementById('btn-save').addEventListener('click', saveSettings);
  document.getElementById('btn-test').addEventListener('click', testConnection);
  document.getElementById('btn-save-recorder').addEventListener('click', saveSettings);
  document.getElementById('btn-log-clear').addEventListener('click', () => {
    const el = document.getElementById('conn-log');
    el.innerHTML = '<span class="log-info">Log cleared. Run &#9654; Test connection to see diagnostic output.</span>';
  });

  // Show installed version
  const manifest = chrome.runtime.getManifest();
  document.getElementById('ver-installed').textContent = `v${manifest.version}`;

  // Check remote version (auth-exempt — fetch direct, no TOTP needed)
  document.getElementById('ver-remote-wrap').style.display = 'inline';
  _checkRemoteVersion(cfg);

  document.getElementById('btn-download-update').addEventListener('click', _downloadUpdate);
});

async function saveSettings() {
  const url     = document.getElementById('api-url').value.replace(/\/$/, '').trim();
  const secret  = document.getElementById('api-secret').value.trim();
  const enabled = document.getElementById('recorder-enabled').checked;
  const skip    = document.getElementById('recorder-skip').value
    .split('\n').map(s => s.trim()).filter(Boolean);

  await new Promise(resolve => {
    chrome.storage.local.set({
      blueprints_api_url:    url,
      blueprints_api_secret: secret,
      recorder_enabled:      enabled,
      recorder_skip:         skip,
    }, resolve);
  });
  _show('settings-status', '✓ Settings saved.', 'ok');
  _show('recorder-status', '', '');
}

// ── Connection log ───────────────────────────────────────────────────────

const _LOG_EL = () => document.getElementById('conn-log');

function _ts() {
  const n = new Date();
  const pad = (v, w=2) => String(v).padStart(w, '0');
  return `${pad(n.getHours())}:${pad(n.getMinutes())}:${pad(n.getSeconds())}.${pad(n.getMilliseconds(), 3)}`;
}

function _logLine(cls, label, msg) {
  const el = _LOG_EL();
  if (!el) return;
  const line = document.createElement('div');
  line.innerHTML =
    `<span class="log-ts">[${_ts()}]</span> ` +
    `<span class="${cls}">${label}</span> ` +
    `<span class="log-data">${_esc(msg)}</span>`;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

function _logSep() {
  const el = _LOG_EL();
  if (!el) return;
  // Clear placeholder text on first real run
  if (el.querySelector('.log-info')) el.innerHTML = '';
  const line = document.createElement('div');
  line.innerHTML = `<span class="log-sep">${'─'.repeat(62)}</span>`;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

function _logInfo(msg)  { _logLine('log-info', '[INFO]', msg); }
function _logStep(msg)  { _logLine('log-step', '[STEP]', msg); }
function _logOk(msg)    { _logLine('log-ok',   '[ OK ]', msg); }
function _logWarn(msg)  { _logLine('log-warn', '[WARN]', msg); }
function _logErr(msg)   { _logLine('log-err',  '[ERR ]', msg); }

function _esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function _truncHex(hex, show=8) {
  if (!hex) return '(empty)';
  return hex.length <= show ? hex : `${hex.slice(0, show)}… (${hex.length} chars)`;
}

// ── Test Connection ───────────────────────────────────────────────────────

async function testConnection() {
  _show('settings-status', 'Testing…', '');
  _logSep();
  _logStep('Test connection started');

  // 1. Read settings
  _logInfo('Reading settings from chrome.storage.local…');
  const cfg = await _load();

  const rawUrl    = cfg.blueprints_api_url    || '';
  const rawSecret = cfg.blueprints_api_secret || '';
  const url       = rawUrl.replace(/\/$/, '').trim();

  _logInfo(`API URL    : ${url || '(not set)'}`);
  if (!url) {
    _logErr('API URL is empty — save your settings first.');
    _show('settings-status', '✗ API URL not set', 'err');
    return;
  }

  if (!rawSecret) {
    _logWarn('API Secret : NOT SET — TOTP token will be empty, request will likely be rejected');
  } else {
    _logInfo(`API Secret : set — ${rawSecret.length} hex chars (${rawSecret.length * 4} bits)`);
  }

  // 2. Compute TOTP
  _logStep('Computing TOTP token…');
  const windowNum = Math.floor(Date.now() / 5000);
  const windowSec = windowNum * 5;
  const windowUtc = new Date(windowSec * 1000).toISOString().replace('T',' ').replace('.000Z',' UTC');
  _logInfo(`TOTP window: ${windowNum}  (epoch second ${windowSec}, ~${windowUtc})`);
  _logInfo(`Window age : ${(Date.now() - windowSec * 1000).toFixed(0)} ms into current 5 s window`);

  let token = '';
  if (rawSecret) {
    try {
      const keyBytes = Uint8Array.from(rawSecret.match(/.{1,2}/g).map(b => parseInt(b, 16)));
      const msgBytes = new TextEncoder().encode(String(windowNum));
      const key = await crypto.subtle.importKey(
        'raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
      );
      const sig = await crypto.subtle.sign('HMAC', key, msgBytes);
      token = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2,'0')).join('');
      _logOk(`TOTP token : ${_truncHex(token)} (HMAC-SHA256, full length ${token.length} chars)`);
    } catch (ex) {
      _logErr(`TOTP computation failed: ${ex.message}`);
      _logWarn('Possible cause: API Secret is not valid hex.');
      _show('settings-status', `✗ TOTP error: ${ex.message}`, 'err');
      return;
    }
  } else {
    _logWarn('Skipping TOTP — secret not set, sending unauthenticated request');
  }

  // 3. Send request
  const endpoint = `${url}/api/v1/bookmarks/health`;
  _logStep(`Sending GET ${endpoint}`);
  if (token) {
    _logInfo(`Request headers: X-API-Token: ${_truncHex(token)}`);
  } else {
    _logWarn('Request headers: X-API-Token: (none)');
  }
  _logInfo(`Browser origin: ${location.origin}`);

  const t0 = performance.now();
  let response;
  try {
    response = await fetch(endpoint, {
      headers: token ? { 'X-API-Token': token } : {},
    });
  } catch (netErr) {
    const elapsed = (performance.now() - t0).toFixed(0);
    _logErr(`Network error after ${elapsed} ms: ${netErr.message}`);
    _logWarn('Possible causes: node unreachable, DNS failure, certificate rejected, CORS preflight blocked.');
    _show('settings-status', `✗ Network error: ${netErr.message}`, 'err');
    return;
  }

  const elapsed = (performance.now() - t0).toFixed(0);
  const statusOk = response.ok;
  const logStatus = statusOk ? _logOk : _logErr;
  logStatus(`← HTTP ${response.status} ${response.statusText}  (${elapsed} ms)`);

  // 4. Log response headers
  _logStep('Response headers:');
  const relevantHeaders = [
    'content-type', 'access-control-allow-origin', 'x-request-id', 'server',
    'via', 'date', 'cache-control',
  ];
  let headersLogged = 0;
  for (const h of relevantHeaders) {
    const v = response.headers.get(h);
    if (v) { _logInfo(`  ${h}: ${v}`); headersLogged++; }
  }
  if (!headersLogged) {
    _logWarn('  (no expected headers received — possible CORS block before response)');
  }
  const corsOrigin = response.headers.get('access-control-allow-origin');
  if (!corsOrigin) {
    // Note: browsers intentionally restrict which response headers are readable
    // by JS — access-control-allow-origin is often present but not JS-readable
    // even when the response was allowed. A successful body response (below)
    // is the definitive proof that CORS was satisfied.
    _logWarn('  access-control-allow-origin: not JS-readable (normal for wildcard CORS) — body success confirms CORS was satisfied');
  } else if (corsOrigin === '*' || corsOrigin === location.origin) {
    _logOk(`  CORS: origin allowed (${corsOrigin})`);
  } else {
    _logWarn(`  CORS: origin allowed for ${corsOrigin} (not this extension) — may still succeed`);
  }

  // 5. Parse body
  _logStep('Parsing response body…');
  let body;
  const rawText = await response.text();
  _logInfo(`Raw body (${rawText.length} chars): ${rawText.slice(0, 400)}${rawText.length > 400 ? '…' : ''}`);

  if (!statusOk) {
    _logErr(`HTTP ${response.status} — request rejected.`);
    const hint = {
      401: 'Unauthorized — TOTP token invalid or secret mismatch.',
      403: 'Forbidden — your IP is not in BLUEPRINTS_ALLOWED_NETWORKS on the node.',
      404: 'Not found — bookmarks feature may not be enabled on this node.',
      500: 'Server error — check journalctl -u blueprints-app on the node.',
    }[response.status];
    if (hint) _logWarn(`Hint: ${hint}`);
    _show('settings-status', `✗ HTTP ${response.status} ${response.statusText}`, 'err');
    return;
  }

  try {
    body = JSON.parse(rawText);
  } catch (parseErr) {
    _logErr(`JSON parse error: ${parseErr.message}`);
    _show('settings-status', '✗ Response is not JSON', 'err');
    return;
  }

  // 6. Report individual fields
  _logStep('Health fields:');
  const fields = [
    ['status',               v => v === 'ok'      ? _logOk  : _logWarn],
    ['sqlite',               v => v === 'ok'      ? _logOk  : _logErr ],
    ['seekdb',               v => v === 'ok'      ? _logOk  : _logWarn],
    ['embedding',            v => v === 'ok'      ? _logOk  : _logWarn],
    ['embedding_error',      v => v ? _logWarn : null],
    ['bookmark_count',       () => _logInfo],
    ['visit_count',          () => _logInfo],
    ['seekdb_indexed',       () => _logInfo],
    ['seekdb_visits_indexed',() => _logInfo],
    ['last_seekdb_sync',     () => _logInfo],
  ];
  for (const [key, levelFn] of fields) {
    if (!(key in body)) continue;
    const val = body[key];
    const logFn = levelFn(val);
    if (logFn) logFn(`  ${key}: ${JSON.stringify(val)}`);
  }

  if (body.seekdb !== 'ok') {
    _logWarn('SeekDB not ok — search may return no results. Check seekdb service on node.');
  }
  if (body.embedding !== 'ok') {
    _logWarn(`Embedding degraded: ${body.embedding_error || 'unknown error'} — vector search disabled.`);
  }
  if (typeof body.bookmark_count === 'number' && body.bookmark_count === 0) {
    _logWarn('bookmark_count is 0 — no bookmarks indexed yet. Import bookmarks via the GUI.');
  }
  if (typeof body.seekdb_indexed === 'number' && typeof body.bookmark_count === 'number') {
    const gap = body.bookmark_count - body.seekdb_indexed;
    if (gap > 0) _logWarn(`SeekDB index gap: ${gap} bookmarks not yet indexed (sync may be in progress).`);
    else if (gap === 0 && body.bookmark_count > 0) _logOk('SeekDB index: fully synced.');
  }

  _logOk(`Test complete — ${elapsed} ms round-trip.`);
  _show('settings-status',
    `✓ Connected — ${body.bookmark_count} bookmarks, SeekDB: ${body.seekdb}, embedding: ${body.embedding}`,
    'ok');
}

// ── Version check & update banner ─────────────────────────────────────────

function _semverGt(a, b) {
  const pa = String(a).split('.').map(Number);
  const pb = String(b).split('.').map(Number);
  for (let i = 0; i < 3; i++) {
    if ((pa[i] || 0) > (pb[i] || 0)) return true;
    if ((pa[i] || 0) < (pb[i] || 0)) return false;
  }
  return false;
}

async function _checkRemoteVersion(cfg) {
  const base = ((cfg && cfg.blueprints_api_url) || '').replace(/\/$/, '');
  const remoteEl  = document.getElementById('ver-remote');
  const banner    = document.getElementById('update-banner');
  const bannerTxt = document.getElementById('update-banner-text');
  const installed = chrome.runtime.getManifest().version;

  if (!base) {
    remoteEl.textContent = '(API URL not set)';
    return;
  }

  try {
    const res = await fetch(`${base}/api/v1/bookmarks/extension-version`, {
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) {
      remoteEl.style.color = 'var(--err)';
      remoteEl.textContent = `HTTP ${res.status}`;
      return;
    }
    const data = await res.json();
    const remoteVer = data.version || '?';
    remoteEl.textContent = `v${remoteVer}`;

    if (_semverGt(remoteVer, installed)) {
      remoteEl.style.color = '#d29922';
      bannerTxt.textContent = `Update available: node has v${remoteVer} (you have v${installed})`;
      banner.style.display = 'block';
    } else {
      remoteEl.style.color = 'var(--ok)';
      banner.style.display = 'none';
    }
  } catch (e) {
    remoteEl.style.color = 'var(--text-dim)';
    remoteEl.textContent = 'unreachable';
  }
}

async function _downloadUpdate() {
  const btn  = document.getElementById('btn-download-update');
  const orig = btn.textContent;
  btn.textContent = '⏳ Downloading…';
  btn.disabled    = true;
  try {
    const res = await apiFetch('/api/v1/bookmarks/extension-download');
    if (!res.ok) { btn.textContent = `✗ HTTP ${res.status}`; return; }
    const blob = await res.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = 'blueprints-bookmarks-extension.zip';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    btn.textContent = '✓ Downloaded';
  } catch (e) {
    btn.textContent = `✗ ${e.message}`;
  } finally {
    setTimeout(() => { btn.textContent = orig; btn.disabled = false; }, 4000);
  }
}
