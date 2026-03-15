/* ── API authentication (TOTP) ──────────────────────────────────────── */
const _LS_SECRET_KEY = 'blueprints_api_secret';

async function _computeApiToken(secretHex) {
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
  } catch (e) {
    return '';
  }
}

async function apiFetch(url, options = {}) {
  const secret = localStorage.getItem(_LS_SECRET_KEY) || '';
  const token  = await _computeApiToken(secret);
  const merged = {
    ...options,
    headers: { ...(options.headers || {}), ...(token ? { 'X-API-Token': token } : {}) },
  };
  const r = await fetch(url, merged);
  if (r.status === 401) { openApiKeyModal(true); }
  return r;
}

function openApiKeyModal(authFailed = false) {
  const modal = document.getElementById('api-key-modal');
  document.getElementById('api-key-failed-msg').hidden = !authFailed;
  document.getElementById('api-key-input').value = localStorage.getItem(_LS_SECRET_KEY) || '';
  modal.showModal();
}

function saveApiKey() {
  const val = document.getElementById('api-key-input').value.trim();
  if (val) {
    localStorage.setItem(_LS_SECRET_KEY, val);
  } else {
    localStorage.removeItem(_LS_SECRET_KEY);
  }
  document.getElementById('api-key-modal').close();
}
