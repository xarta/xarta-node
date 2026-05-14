/* shared/api.js — TOTP auth matching blueprints GUI (gui-fallback/js/api.js) */

const _BM_STORAGE_API_URL    = 'blueprints_api_url';
const _BM_STORAGE_API_SECRET = 'blueprints_api_secret';
const _BM_TIME_TTL_MS        = 5 * 60 * 1000;

const _bmTimeOffsets = new Map();
const _bmTimeInflight = new Map();

async function _getSettings() {
  return new Promise(resolve => {
    chrome.storage.local.get(
      [_BM_STORAGE_API_URL, _BM_STORAGE_API_SECRET],
      data => resolve(data)
    );
  });
}

async function _syncApiTime(baseUrl, force = false) {
  const base = (baseUrl || '').replace(/\/$/, '');
  if (!base) return 0;
  const cached = _bmTimeOffsets.get(base);
  if (!force && cached && (Date.now() - cached.syncedAt) <= _BM_TIME_TTL_MS) {
    return cached.offsetMs;
  }
  if (_bmTimeInflight.has(base)) return _bmTimeInflight.get(base);

  const promise = (async () => {
    const startedAt = Date.now();
    try {
      const resp = await fetch(`${base}/api/v1/auth/time`, { cache: 'no-store' });
      const endedAt = Date.now();
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      const serverMs = Number(data && data.server_epoch_ms);
      if (!Number.isFinite(serverMs)) throw new Error('missing server_epoch_ms');
      const offsetMs = Math.round((serverMs + ((endedAt - startedAt) / 2)) - endedAt);
      _bmTimeOffsets.set(base, { offsetMs, syncedAt: Date.now() });
      return offsetMs;
    } catch (_) {
      return cached && typeof cached.offsetMs === 'number' ? cached.offsetMs : 0;
    } finally {
      _bmTimeInflight.delete(base);
    }
  })();

  _bmTimeInflight.set(base, promise);
  return promise;
}

async function _computeToken(secretHex, baseUrl, opts = {}) {
  if (!secretHex) return '';
  try {
    const offsetMs = await _syncApiTime(baseUrl, !!opts.forceTimeSync);
    const windowNum = Math.floor((Date.now() + offsetMs) / 5000);
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
  const url    = base + path;
  const token  = await _computeToken(secret, base);
  const response = await fetch(url, {
    ...options,
    headers: {
      ...(options.headers || {}),
      ...(token ? { 'X-API-Token': token } : {}),
    },
  });
  if (response.status !== 401 || !token) return response;

  const retryToken = await _computeToken(secret, base, { forceTimeSync: true });
  if (!retryToken) return response;
  return fetch(url, {
    ...options,
    headers: {
      ...(options.headers || {}),
      'X-API-Token': retryToken,
    },
  });
}
