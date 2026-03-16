/* ── TODO ─────────────────────────────────────────────────────────────── */
let _todoLoaded = false;
let _todoPreview = false;

async function loadTodo() {
  const err    = document.getElementById('todo-error');
  const editor = document.getElementById('todo-editor');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/todo');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    editor.value = d.content || '';
    _todoLoaded = true;
    if (_todoPreview) _renderTodoPreview();
  } catch (e) {
    err.textContent = `Failed to load: ${e.message}`;
    err.hidden = false;
  }
}

async function todoSave() {
  const btn    = document.getElementById('todo-save-btn');
  const status = document.getElementById('todo-status');
  const editor = document.getElementById('todo-editor');
  btn.disabled = true;
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/todo', {
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

function todoTogglePreview() {
  _todoPreview = !_todoPreview;
  const btn     = document.getElementById('todo-preview-btn');
  const editor  = document.getElementById('todo-editor');
  const preview = document.getElementById('todo-preview');
  if (_todoPreview) {
    _renderTodoPreview();
    editor.style.display  = 'none';
    preview.style.display = 'block';
    btn.textContent = '\u270f Edit';
  } else {
    preview.style.display = 'none';
    editor.style.display  = 'block';
    btn.textContent = '\ud83d\udc41 Preview';
  }
}

function _renderTodoPreview() {
  const src     = document.getElementById('todo-editor').value;
  const preview = document.getElementById('todo-preview');
  preview.innerHTML = _mdToHtml(src);
}
