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

async function testConnection() {
  _show('settings-status', 'Testing…', '');
  try {
    const r = await apiFetch('/api/v1/bookmarks/health');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    _show('settings-status',
      `✓ Connected — ${d.bookmark_count} bookmarks, SeekDB: ${d.seekdb}, embedding: ${d.embedding}`,
      'ok');
  } catch (e) {
    _show('settings-status', `✗ Connection failed: ${e.message}`, 'err');
  }
}
