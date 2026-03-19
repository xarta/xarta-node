/* service-worker.js — Blueprints Bookmarks Extension background worker
 *
 * Responsibilities:
 *  - Register context menu items (save page / save link)
 *  - Passive visit recorder: POST to /api/v1/bookmarks/visits on tab navigation
 *    (only when enabled in options)
 */

importScripts('shared/api.js');

// ── Context menu ──────────────────────────────────────────────────────────

chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({ id: 'save-page', title: 'Save page to Blueprints', contexts: ['page'] });
  chrome.contextMenus.create({ id: 'save-link', title: 'Save link to Blueprints', contexts: ['link'] });
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
