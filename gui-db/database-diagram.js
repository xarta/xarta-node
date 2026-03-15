let mermaidText = '';

let mermaidReady = false;

function setRenderStatus(message, isError = false) {
  const el = document.getElementById('render-status');
  if (!el) return;
  el.textContent = message;
  el.classList.toggle('error', !!isError);
}

function loadMermaidLibrary() {
  if (window.mermaid) {
    mermaidReady = true;
    return Promise.resolve();
  }

  return new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js';
    script.async = true;
    script.onload = () => {
      try {
        window.mermaid.initialize({
          startOnLoad: false,
          theme: 'dark',
          securityLevel: 'loose',
        });
        mermaidReady = true;
        resolve();
      } catch (error) {
        reject(error);
      }
    };
    script.onerror = () => reject(new Error('Failed to load Mermaid renderer'));
    document.head.appendChild(script);
  });
}

async function renderMermaidSvg() {
  const renderRoot = document.getElementById('mermaid-render');
  if (!renderRoot) return;

  if (!mermaidText) {
    renderRoot.innerHTML = '';
    setRenderStatus('No Mermaid source available.', true);
    return;
  }

  try {
    if (!mermaidReady) {
      setRenderStatus('Loading Mermaid renderer…');
      await loadMermaidLibrary();
    }

    const renderId = `bp-mermaid-${Date.now()}`;
    const result = await window.mermaid.render(renderId, mermaidText);
    renderRoot.innerHTML = result.svg;
    setRenderStatus('Rendered Mermaid diagram from live schema.');
  } catch (error) {
    renderRoot.innerHTML = '';
    setRenderStatus(`Renderer unavailable (${error.message}). Showing source below.`, true);
  }
}

function esc(value) {
  return String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function renderRelationships(relationships) {
  const root = document.getElementById('relationships');
  root.innerHTML = (relationships || []).length
    ? relationships.map(r => `<li><code>${esc(r.source_table)}.${esc(r.source_column)}</code> → <code>${esc(r.target_table)}.${esc(r.target_column)}</code> (${esc(r.kind)})</li>`).join('')
    : '<li>No relationships detected.</li>';
}

async function loadDiagram() {
  const status = document.getElementById('status');
  status.textContent = 'Loading schema…';
  status.classList.remove('error');

  try {
    const _fetch = window.apiFetch || fetch;
    const response = await _fetch('/api/v1/schema');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    mermaidText = data.mermaid || 'erDiagram\n    EMPTY ||--|| EMPTY : "no data"';
    document.getElementById('mermaid-source').textContent = mermaidText;
    renderRelationships(data.relationships);
    status.textContent = 'Mermaid diagram source generated from live schema.';
    await renderMermaidSvg();
  } catch (error) {
    status.textContent = `Failed to load schema: ${error.message}`;
    status.classList.add('error');
    setRenderStatus('Unable to render diagram because schema fetch failed.', true);
  }
}

function copyMermaid() {
  if (!mermaidText) return;
  navigator.clipboard.writeText(mermaidText).then(() => {
    const copied = document.getElementById('copied');
    copied.textContent = 'Copied.';
    setTimeout(() => { copied.textContent = ''; }, 1500);
  }).catch(() => {
    const copied = document.getElementById('copied');
    copied.textContent = 'Copy failed.';
    setTimeout(() => { copied.textContent = ''; }, 1500);
  });
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('copy-btn').addEventListener('click', copyMermaid);
  loadDiagram();
});
