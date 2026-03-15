/* ── Basic Assumptions ────────────────────────────────────────────────── */
let _assumptionsLoaded = false;
let _assumptionsPreview = false;

async function loadAssumptions() {
  const err    = document.getElementById('assumptions-error');
  const editor = document.getElementById('assumptions-editor');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/assumptions');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    editor.value = d.content || '';
    _assumptionsLoaded = true;
    if (_assumptionsPreview) _renderAssumptionsPreview();
  } catch (e) {
    err.textContent = `Failed to load: ${e.message}`;
    err.hidden = false;
  }
}

async function assumptionsSave() {
  const btn    = document.getElementById('assumptions-save-btn');
  const status = document.getElementById('assumptions-status');
  const editor = document.getElementById('assumptions-editor');
  btn.disabled = true;
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/assumptions', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: editor.value }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    status.textContent = '\u2713 Saved';
    status.style.color = 'var(--accent)';
    status.hidden = false;
    setTimeout(() => { status.hidden = true; }, 3000);
  } catch (e) {
    status.textContent = `\u2717 Save failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
  }
}

function assumptionsTogglePreview() {
  _assumptionsPreview = !_assumptionsPreview;
  const btn     = document.getElementById('assumptions-preview-btn');
  const editor  = document.getElementById('assumptions-editor');
  const preview = document.getElementById('assumptions-preview');
  if (_assumptionsPreview) {
    _renderAssumptionsPreview();
    editor.style.display  = 'none';
    preview.style.display = 'block';
    btn.textContent = '\u270f Edit';
  } else {
    preview.style.display = 'none';
    editor.style.display  = 'block';
    btn.textContent = '\ud83d\udc41 Preview';
  }
}

function _renderAssumptionsPreview() {
  const src     = document.getElementById('assumptions-editor').value;
  const preview = document.getElementById('assumptions-preview');
  preview.innerHTML = _mdToHtml(src);
}

function _mdToHtml(md) {
  // Minimal but functional markdown renderer — no external deps
  const esc2 = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const lines = md.split('\n');
  let html = '';
  let inUl = false, inOl = false, inCode = false, codeLang = '', codeBuf = '';
  const closeList = () => {
    if (inUl) { html += '</ul>'; inUl = false; }
    if (inOl) { html += '</ol>'; inOl = false; }
  };
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    // Fenced code blocks
    if (!inCode && /^```/.test(line)) {
      closeList();
      inCode = true; codeLang = line.slice(3).trim(); codeBuf = ''; continue;
    }
    if (inCode) {
      if (/^```/.test(line)) {
        const langAttr = codeLang ? ` class="language-${esc2(codeLang)}"` : '';
        html += `<pre style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;overflow-x:auto;font-size:12.5px;line-height:1.5"><code${langAttr}>${esc2(codeBuf)}</code></pre>`;
        inCode = false; codeBuf = '';
      } else { codeBuf += line + '\n'; }
      continue;
    }
    // Headings
    const hm = line.match(/^(#{1,6})\s+(.*)/);
    if (hm) {
      closeList();
      const lvl = hm[1].length;
      const sizes = ['1.6em','1.35em','1.15em','1em','0.95em','0.9em'];
      const margins = ['24px 0 10px','20px 0 8px','16px 0 6px','14px 0 4px','12px 0 4px','10px 0 4px'];
      html += `<h${lvl} style="font-size:${sizes[lvl-1]};font-weight:700;margin:${margins[lvl-1]};color:var(--text);border-bottom:${lvl<=2?'1px solid var(--border)':'none'};padding-bottom:${lvl<=2?'6px':'0'}">${_inlineMd(hm[2])}</h${lvl}>`;
      continue;
    }
    // Horizontal rule
    if (/^(\*{3,}|-{3,}|_{3,})$/.test(line.trim())) {
      closeList();
      html += '<hr style="border:none;border-top:1px solid var(--border);margin:16px 0">';
      continue;
    }
    // Unordered list
    const ulm = line.match(/^(\s*)[-*+]\s+(.*)/);
    if (ulm) {
      if (!inUl) { closeList(); html += '<ul style="margin:6px 0 6px 20px;padding:0">'; inUl = true; }
      html += `<li style="margin:3px 0">${_inlineMd(ulm[2])}</li>`;
      continue;
    }
    // Ordered list
    const olm = line.match(/^(\s*)\d+\.\s+(.*)/);
    if (olm) {
      if (!inOl) { closeList(); html += '<ol style="margin:6px 0 6px 20px;padding:0">'; inOl = true; }
      html += `<li style="margin:3px 0">${_inlineMd(olm[2])}</li>`;
      continue;
    }
    // Blockquote
    const bqm = line.match(/^>\s*(.*)/);
    if (bqm) {
      closeList();
      html += `<blockquote style="margin:8px 0;padding:8px 14px;border-left:3px solid var(--accent);background:var(--surface);color:var(--text-dim);font-style:italic">${_inlineMd(bqm[1])}</blockquote>`;
      continue;
    }
    // Empty line
    closeList();
    if (!line.trim()) { html += '<div style="height:8px"></div>'; continue; }
    // Paragraph
    html += `<p style="margin:4px 0">${_inlineMd(line)}</p>`;
  }
  if (inCode) html += `<pre style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;overflow-x:auto"><code>${esc2(codeBuf)}</code></pre>`;
  closeList();
  return html;
}

function _inlineMd(s) {
  // inline: code, bold, italic, links, strikethrough
  const e = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const parts = []; let buf = '', i = 0;
  // Simple state-machine via regex replacements (no nesting)
  return s
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/`([^`]+)`/g, (_,c) => `<code style="background:var(--surface);border:1px solid var(--border);border-radius:3px;padding:1px 5px;font-size:0.88em">${c.replace(/&lt;/g,'<').replace(/&gt;/g,'>')}</code>`)
    .replace(/\*\*\*([^*]+)\*\*\*/g, '<strong><em>$1</em></strong>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/__([^_]+)__/g, '<strong>$1</strong>')
    .replace(/\*([^*]+)\*/g, '<em>$1</em>')
    .replace(/_([^_]+)_/g, '<em>$1</em>')
    .replace(/~~([^~]+)~~/g, '<del>$1</del>')
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" style="color:var(--accent);text-decoration:underline" target="_blank" rel="noopener noreferrer">$1</a>');
}
