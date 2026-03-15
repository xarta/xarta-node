let schemaData = null;

function esc(value) {
  return String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function notesHtml(notes) {
  if (!notes || !notes.length) return '—';
  return notes.map(note => `<span class="badge">${esc(note)}</span>`).join(' ');
}

function nullableText(nullable) {
  return nullable ? '✓' : '✗';
}

function defaultText(value) {
  return value === null || value === undefined ? '—' : esc(value);
}

function renderTables() {
  const root = document.getElementById('tables-root');
  if (!schemaData) {
    root.innerHTML = '<div class="panel status">No schema data available.</div>';
    return;
  }

  root.innerHTML = schemaData.tables.map(table => {
    const rows = table.columns.map(col => `
      <tr>
        <td><code>${esc(col.column)}</code></td>
        <td><code>${esc(col.type)}</code></td>
        <td>${nullableText(col.nullable)}</td>
        <td>${defaultText(col.default)}</td>
        <td>${notesHtml(col.notes)}</td>
      </tr>`).join('');

    return `
      <section class="panel">
        <h2>${esc(table.table)}</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Column</th>
                <th>Type</th>
                <th>Null?</th>
                <th>Default</th>
                <th>Notes</th>
              </tr>
            </thead>
            <tbody>
              ${rows}
            </tbody>
          </table>
        </div>
      </section>`;
  }).join('');

  const rel = schemaData.relationships || [];
  document.getElementById('relationships').innerHTML = rel.length
    ? rel.map(r => `<li><code>${esc(r.source_table)}.${esc(r.source_column)}</code> → <code>${esc(r.target_table)}.${esc(r.target_column)}</code> (${esc(r.kind)})</li>`).join('')
    : '<li>No relationships detected.</li>';
}

async function loadSchema() {
  const status = document.getElementById('status');
  status.textContent = 'Loading schema…';
  status.classList.remove('error');

  try {
    const _fetch = window.apiFetch || fetch;
    const response = await _fetch('/api/v1/schema');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    schemaData = await response.json();
    status.textContent = `Loaded ${schemaData.tables.length} tables.`;
    renderTables();
  } catch (error) {
    status.textContent = `Failed to load schema: ${error.message}`;
    status.classList.add('error');
  }
}

document.addEventListener('DOMContentLoaded', loadSchema);
