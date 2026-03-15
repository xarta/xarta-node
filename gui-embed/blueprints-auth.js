/**
 * blueprints-auth.js — shared TOTP authentication for standalone Blueprints pages.
 *
 * Defines window._computeApiToken and window.apiFetch using the same
 * localStorage secret as the main Blueprints GUI (key: 'blueprints_api_secret').
 *
 * Include this before blueprints-node-selector.js and any JS that calls fetch()
 * on the Blueprints API.  The main index.html defines its own apiFetch (with a
 * full modal UI for 401s) so this file's apiFetch is only installed if
 * window.apiFetch is not already defined.
 */
(function () {
  'use strict';
  if (typeof window === 'undefined') return;

  const LS_SECRET = 'blueprints_api_secret';

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
    } catch { return ''; }
  }

  async function apiFetch(url, options = {}) {
    const secret = localStorage.getItem(LS_SECRET) || '';
    const token  = await _computeApiToken(secret);
    const merged = {
      ...options,
      headers: { ...(options.headers || {}), ...(token ? { 'X-API-Token': token } : {}) },
    };
    const r = await fetch(url, merged);
    if (r.status === 401) {
      // Prompt for key — same localStorage slot as the main GUI so the key carries over
      const entered = prompt(
        'Blueprints API key required (401 Unauthorized).\n' +
        'Enter your API secret (same key used in the main Blueprints UI):'
      );
      if (entered && entered.trim()) {
        localStorage.setItem(LS_SECRET, entered.trim());
        const token2  = await _computeApiToken(entered.trim());
        const merged2 = {
          ...options,
          headers: { ...(options.headers || {}), ...(token2 ? { 'X-API-Token': token2 } : {}) },
        };
        return fetch(url, merged2);
      }
    }
    return r;
  }

  // Always expose _computeApiToken; only set apiFetch if the main GUI hasn't
  window._computeApiToken = _computeApiToken;
  if (!window.apiFetch) window.apiFetch = apiFetch;
})();
