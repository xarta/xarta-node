/* shared/api.js — TOTP auth matching blueprints GUI (gui-fallback/js/api.js) */

const _BM_STORAGE_API_URL    = 'blueprints_api_url';
const _BM_STORAGE_API_SECRET = 'blueprints_api_secret';

async function _getSettings() {
  return new Promise(resolve => {
    chrome.storage.local.get(
      [_BM_STORAGE_API_URL, _BM_STORAGE_API_SECRET],
      data => resolve(data)
    );
  });
}

async function _computeToken(secretHex) {
  if (!secretHex) return '';
  try {
    const windowNum = Math.floor(Date.now() / 5000);
    const keyBytes  = Uint8Array.from(secretHex.match(/.{1,2}/g).map(b => parseInt(b, 16)));
    const msgBytes  = new TextEncoder().encode(String(windowNum));
    const key = await crypto.subtle.importKey(
      'raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
    );
    const sig = await crypto.subtle.sign('HMAC', key, msgBytes);
    return Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');
  } catch (_) {
    return '';
  }
}

async function apiFetch(path, options = {}) {
  const cfg    = await _getSettings();
  const base   = (cfg[_BM_STORAGE_API_URL] || '').replace(/\/$/, '');
  const secret = cfg[_BM_STORAGE_API_SECRET] || '';
  const token  = await _computeToken(secret);
  const url    = base + path;
  return fetch(url, {
    ...options,
    headers: {
      ...(options.headers || {}),
      ...(token ? { 'X-API-Token': token } : {}),
    },
  });
}
