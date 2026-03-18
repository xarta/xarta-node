/* ── Docs navigation history (browser-local) ───────────────────────────── */

const _DOCS_HIST_LS_KEY = 'bp_docs_nav_history_v1';
const _DOCS_HIST_MAX = 200;

let _docsHistState = {
  back: [],
  current: null,
  forward: [],
};

function _docsHistLoad() {
  try {
    const raw = localStorage.getItem(_DOCS_HIST_LS_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    const back = Array.isArray(parsed.back) ? parsed.back.filter(v => typeof v === 'string' && v) : [];
    const current = typeof parsed.current === 'string' && parsed.current ? parsed.current : null;
    const forward = Array.isArray(parsed.forward) ? parsed.forward.filter(v => typeof v === 'string' && v) : [];
    _docsHistState = { back, current, forward };
    _docsHistTrim();
  } catch (_) {
    _docsHistState = { back: [], current: null, forward: [] };
  }
}

function _docsHistSave() {
  _docsHistTrim();
  localStorage.setItem(_DOCS_HIST_LS_KEY, JSON.stringify(_docsHistState));
}

function _docsHistTrim() {
  // Maintain a capped, bounded history footprint.
  while ((_docsHistState.back.length + _docsHistState.forward.length + (_docsHistState.current ? 1 : 0)) > _DOCS_HIST_MAX) {
    if (_docsHistState.back.length) {
      _docsHistState.back.shift();
    } else if (_docsHistState.forward.length) {
      _docsHistState.forward.pop();
    } else {
      break;
    }
  }
}

function docsHistInit(activeDocId) {
  _docsHistLoad();
  if (!_docsHistState.current && typeof activeDocId === 'string' && activeDocId) {
    _docsHistState.current = activeDocId;
    _docsHistSave();
  }
}

function docsHistCurrent() {
  return _docsHistState.current;
}

function docsHistRecordDirect(docId) {
  if (!docId || typeof docId !== 'string') return;
  if (_docsHistState.current === docId) return;

  if (_docsHistState.current) {
    _docsHistState.back.push(_docsHistState.current);
  }
  _docsHistState.current = docId;
  // Browser-style branch behavior: new direct visit drops forward trail.
  _docsHistState.forward = [];
  _docsHistSave();
}

function docsHistCanBack() {
  return _docsHistState.back.length > 0;
}

function docsHistCanForward() {
  return _docsHistState.forward.length > 0;
}

function docsHistPeekBack() {
  return docsHistCanBack() ? _docsHistState.back[_docsHistState.back.length - 1] : null;
}

function docsHistPeekForward() {
  return docsHistCanForward() ? _docsHistState.forward[_docsHistState.forward.length - 1] : null;
}

function docsHistStepBack() {
  if (!docsHistCanBack()) return null;
  if (_docsHistState.current) {
    _docsHistState.forward.push(_docsHistState.current);
  }
  _docsHistState.current = _docsHistState.back.pop() || null;
  _docsHistSave();
  return _docsHistState.current;
}

function docsHistStepForward() {
  if (!docsHistCanForward()) return null;
  if (_docsHistState.current) {
    _docsHistState.back.push(_docsHistState.current);
  }
  _docsHistState.current = _docsHistState.forward.pop() || null;
  _docsHistSave();
  return _docsHistState.current;
}

function docsHistRemoveDoc(docId) {
  if (!docId) return _docsHistState.current;
  _docsHistState.back = _docsHistState.back.filter(v => v !== docId);
  _docsHistState.forward = _docsHistState.forward.filter(v => v !== docId);
  if (_docsHistState.current === docId) {
    _docsHistState.current = _docsHistState.back.pop() || _docsHistState.forward.pop() || null;
  }
  _docsHistSave();
  return _docsHistState.current;
}

function docsHistStats() {
  return {
    back: _docsHistState.back.length,
    forward: _docsHistState.forward.length,
    current: _docsHistState.current,
    max: _DOCS_HIST_MAX,
  };
}
