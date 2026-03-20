/* service-worker.js — Blueprints Bookmarks Extension background worker
 *
 * Responsibilities:
 *  - Register context menu items (save page / save link)
 *  - Passive visit recorder: POST to /api/v1/bookmarks/visits on tab navigation
 *    (only when enabled in options)
 *  - Periodic version check: badge "↑" when the node has a newer extension build
 */

importScripts('shared/api.js');

// ── Version check ─────────────────────────────────────────────────────────

function _semverGt(a, b) {
  const pa = String(a).split('.').map(Number);
  const pb = String(b).split('.').map(Number);
  for (let i = 0; i < 3; i++) {
    if ((pa[i] || 0) > (pb[i] || 0)) return true;
    if ((pa[i] || 0) < (pb[i] || 0)) return false;
  }
  return false;
}

async function _checkVersion() {
  const data = await new Promise(resolve =>
    chrome.storage.local.get(['blueprints_api_url'], resolve)
  );
  const base = (data.blueprints_api_url || '').replace(/\/$/, '');
  if (!base) return;

  try {
    const res = await fetch(`${base}/api/v1/bookmarks/extension-version`, {
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) return;
    const json = await res.json();
    const remoteVersion = json.version || '';
    const localVersion  = chrome.runtime.getManifest().version;

    if (remoteVersion && _semverGt(remoteVersion, localVersion)) {
      chrome.action.setBadgeText({ text: '↑' });
      chrome.action.setBadgeBackgroundColor({ color: '#d29922' });
      chrome.storage.local.set({
        blueprints_update_available: true,
        blueprints_update_version:   remoteVersion,
      });
    } else {
      chrome.action.setBadgeText({ text: '' });
      chrome.storage.local.remove(['blueprints_update_available', 'blueprints_update_version']);
    }
  } catch (_) {
    // Network error — silently ignore so this never disrupts normal use
  }
}

// ── Context menu ──────────────────────────────────────────────────────────

chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({ id: 'save-page', title: 'Save page to Blueprints', contexts: ['page'] });
  chrome.contextMenus.create({ id: 'save-link', title: 'Save link to Blueprints', contexts: ['link'] });

  // Clear any stale update flag; register periodic version check (every 4 hours)
  chrome.storage.local.remove(['blueprints_update_available', 'blueprints_update_version']);
  chrome.action.setBadgeText({ text: '' });
  chrome.alarms.create('blueprints-version-check', { delayInMinutes: 1, periodInMinutes: 240 });
  _checkVersion();
});

chrome.runtime.onStartup.addListener(() => {
  // Re-register alarm in case it was cleared; restore badge if update was pending
  chrome.alarms.create('blueprints-version-check', { delayInMinutes: 5, periodInMinutes: 240 });
  chrome.storage.local.get(['blueprints_update_available'], d => {
    if (d.blueprints_update_available) {
      chrome.action.setBadgeText({ text: '↑' });
      chrome.action.setBadgeBackgroundColor({ color: '#d29922' });
    }
  });
});

chrome.alarms.onAlarm.addListener(alarm => {
  if (alarm.name === 'blueprints-version-check') _checkVersion();
});

chrome.contextMenus.onClicked.addListener((info, tab) => {
  if (info.menuItemId === 'save-page')
    _saveBookmark(tab.url, tab.title, 'context-menu');
  if (info.menuItemId === 'save-link')
    _saveBookmark(info.linkUrl, info.linkUrl, 'context-menu');
});

async function _saveBookmark(url, title, source) {
  if (!url || /^(javascript:|about:)/i.test(url)) return;
  try {
    await apiFetch('/api/v1/bookmarks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, title: title || null, source }),
    });
  } catch (_) {}
}

// ── Visit recorder ────────────────────────────────────────────────────────

const _DEFAULT_SKIP_PREFIXES = ['localhost', '127.', '192.168.', '10.', '172.', 'chrome://', 'edge://'];

async function _isRecorderEnabled() {
  return new Promise(resolve => {
    chrome.storage.local.get(['recorder_enabled', 'recorder_skip'], data => {
      resolve({ enabled: !!data.recorder_enabled, skip: data.recorder_skip || _DEFAULT_SKIP_PREFIXES });
    });
  });
}

chrome.tabs.onUpdated.addListener(async (tabId, changeInfo, tab) => {
  if (changeInfo.status !== 'complete') return;
  const url = tab.url || '';
  if (!url) return;

  const { enabled, skip } = await _isRecorderEnabled();
  if (!enabled) return;

  // Skip URLs that match any denylist prefix
  if (skip.some(prefix => url.startsWith(prefix) || url.includes(`//${prefix}`))) return;

  // Fire-and-forget — visit record loss is acceptable
  apiFetch('/api/v1/bookmarks/visits', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url, title: tab.title || null, source: 'visit-recorder' }),
  }).catch(() => {});
});
