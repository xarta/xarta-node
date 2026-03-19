/* ── AI Providers + Project Assignments ─────────────────────────────── */

async function loadAiProviders() {
  const err = document.getElementById('ai-providers-error');
  if (err) err.hidden = true;
  try {
    const [rp, ra] = await Promise.all([
      apiFetch('/api/v1/ai-providers'),
      apiFetch('/api/v1/ai-project-assignments'),
    ]);
    if (!rp.ok) throw new Error(`Providers HTTP ${rp.status}`);
    if (!ra.ok) throw new Error(`Assignments HTTP ${ra.status}`);
    _aiProviders   = await rp.json();
    _aiAssignments = await ra.json();
    renderAiProviders();
    renderAiAssignments();
  } catch (e) {
    if (err) { err.textContent = `Failed to load AI providers: ${e.message}`; err.hidden = false; }
  }
}

function _typeIcon(type) {
  if (type === 'llm')       return '&#129504;';  // brain
  if (type === 'embedding') return '&#128203;';  // clipboard
  if (type === 'reranker')  return '&#128270;';  // magnifier
  return '&#129302;';
}

function renderAiProviders() {
  const tbody = document.getElementById('ai-providers-tbody');
  if (!tbody) return;
  if (!_aiProviders.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="7">No providers yet — click "+ Add provider" to add one.</td></tr>';
    return;
  }
  tbody.innerHTML = _aiProviders.map(p => `<tr>
    <td title="${esc(p.model_type)}">${_typeIcon(p.model_type)} ${esc(p.model_type)}</td>
    <td><strong>${esc(p.name)}</strong></td>
    <td><code>${esc(p.model_name)}</code></td>
    <td style="text-align:right">${p.dimensions ?? '—'}</td>
    <td style="text-align:center">${p.enabled ? '&#9989;' : '&#10060;'}</td>
    <td style="color:var(--text-dim);font-size:12px">${esc(p.notes || '')}</td>
    <td style="white-space:nowrap">
      <button class="secondary" style="padding:1px 6px;font-size:11px"
        onclick="openAiProviderModal('${esc(p.provider_id)}')">&#9998; Edit</button>
      <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:4px"
        onclick="deleteAiProvider('${esc(p.provider_id)}','${esc(p.name)}')">&#x2715;</button>
    </td>
  </tr>`).join('');
}

function renderAiAssignments() {
  const tbody = document.getElementById('ai-assignments-tbody');
  if (!tbody) return;
  if (!_aiAssignments.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="6">No assignments yet.</td></tr>';
    return;
  }
  const providerMap = Object.fromEntries(_aiProviders.map(p => [p.provider_id, p]));
  tbody.innerHTML = _aiAssignments.map(a => {
    const prov = providerMap[a.provider_id];
    const provLabel = prov ? `${esc(prov.model_name)} <span style="color:var(--text-dim);font-size:11px">(${esc(prov.name)})</span>` : esc(a.provider_id.slice(0,8));
    return `<tr>
      <td><strong>${esc(a.project_name)}</strong></td>
      <td>${_typeIcon(a.role)} ${esc(a.role)}</td>
      <td>${provLabel}</td>
      <td style="text-align:right">${a.priority}</td>
      <td style="text-align:center">${a.enabled ? '&#9989;' : '&#10060;'}</td>
      <td style="white-space:nowrap">
        <button class="secondary" style="padding:1px 6px;font-size:11px"
          onclick="openAiAssignmentModal('${esc(a.assignment_id)}')">&#9998; Edit</button>
        <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:4px"
          onclick="deleteAiAssignment('${esc(a.assignment_id)}','${esc(a.project_name)}','${esc(a.role)}')">&#x2715;</button>
      </td>
    </tr>`;
  }).join('');
}

/* ── Provider modal ─────────────────────────────────────────────────── */

function openAiProviderModal(provider_id) {
  const existing = provider_id ? _aiProviders.find(p => p.provider_id === provider_id) : null;
  const overlay = document.createElement('div');
  overlay.id = 'ai-provider-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:1000;display:flex;align-items:center;justify-content:center';
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
         padding:24px;width:480px;max-width:95vw;max-height:90vh;overflow-y:auto">
      <h3 style="margin:0 0 16px">${existing ? 'Edit' : 'Add'} AI Provider</h3>
      <div style="display:flex;flex-direction:column;gap:10px">
        <label style="font-size:12px;font-weight:600">Name</label>
        <input id="aip-name" type="text" value="${esc(existing?.name || '')}" placeholder="e.g. Local GPU LLM" />
        <label style="font-size:12px;font-weight:600">Base URL</label>
        <input id="aip-url" type="text" value="${esc(existing?.base_url || '')}" placeholder="https://…" />
        <label style="font-size:12px;font-weight:600">API Key</label>
        <input id="aip-key" type="text" value="${esc(existing?.api_key || '')}" placeholder="Bearer token" />
        <label style="font-size:12px;font-weight:600">Model name (stable alias)</label>
        <input id="aip-model" type="text" value="${esc(existing?.model_name || '')}" placeholder="PRIMARY-LOCAL" />
        <label style="font-size:12px;font-weight:600">Model type</label>
        <select id="aip-type">
          <option value="llm"       ${existing?.model_type==='llm'       ?'selected':''}>llm</option>
          <option value="embedding" ${existing?.model_type==='embedding' ?'selected':''}>embedding</option>
          <option value="reranker"  ${existing?.model_type==='reranker'  ?'selected':''}>reranker</option>
        </select>
        <label style="font-size:12px;font-weight:600">Dimensions <span style="font-weight:400;color:var(--text-dim)">(embedding only)</span></label>
        <input id="aip-dims" type="number" value="${existing?.dimensions ?? ''}" placeholder="e.g. 2048" />
        <label style="font-size:12px;font-weight:600">Options JSON <span style="font-weight:400;color:var(--text-dim)">(verify_tls, timeout, no_think_supported…)</span></label>
        <textarea id="aip-opts" rows="3" style="font-family:monospace;font-size:12px;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:var(--radius);padding:6px;resize:vertical">${esc(existing?.options || '{"verify_tls":false}')}</textarea>
        <label style="font-size:12px;font-weight:600">Notes</label>
        <input id="aip-notes" type="text" value="${esc(existing?.notes || '')}" />
        <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
          <input type="checkbox" id="aip-enabled" ${!existing || existing.enabled ? 'checked' : ''}> Enabled
        </label>
      </div>
      <div style="display:flex;gap:8px;margin-top:18px;justify-content:flex-end">
        <button class="secondary" onclick="document.getElementById('ai-provider-overlay').remove()">Cancel</button>
        <button onclick="submitAiProviderModal('${provider_id || ''}')">&#128190; Save</button>
      </div>
      <p id="aip-error" class="error-msg" hidden style="margin-top:8px"></p>
    </div>`;
  document.body.appendChild(overlay);
}

async function submitAiProviderModal(provider_id) {
  const name     = document.getElementById('aip-name').value.trim();
  const base_url = document.getElementById('aip-url').value.trim();
  const api_key  = document.getElementById('aip-key').value.trim();
  const model_name = document.getElementById('aip-model').value.trim();
  const model_type = document.getElementById('aip-type').value;
  const dims_raw   = document.getElementById('aip-dims').value.trim();
  const options    = document.getElementById('aip-opts').value.trim();
  const notes      = document.getElementById('aip-notes').value.trim();
  const enabled    = document.getElementById('aip-enabled').checked;
  const errEl      = document.getElementById('aip-error');

  if (!name || !base_url || !model_name) {
    errEl.textContent = 'Name, base URL, and model name are required.';
    errEl.hidden = false;
    return;
  }
  const body = { name, base_url, api_key, model_name, model_type, enabled,
    dimensions: dims_raw ? parseInt(dims_raw, 10) : null,
    options: options || null,
    notes: notes || null,
  };
  try {
    const r = await apiFetch(
      provider_id ? `/api/v1/ai-providers/${encodeURIComponent(provider_id)}` : '/api/v1/ai-providers',
      { method: provider_id ? 'PUT' : 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) }
    );
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    document.getElementById('ai-provider-overlay').remove();
    _aiProviders = [];
    await loadAiProviders();
  } catch (e) {
    errEl.textContent = `Save failed: ${e.message}`;
    errEl.hidden = false;
  }
}

async function deleteAiProvider(provider_id, name) {
  if (!confirm(`Delete provider "${name}"?\n\nAny project assignments using this provider will be orphaned.`)) return;
  try {
    const r = await apiFetch(`/api/v1/ai-providers/${encodeURIComponent(provider_id)}`, { method: 'DELETE' });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    _aiProviders = _aiProviders.filter(p => p.provider_id !== provider_id);
    renderAiProviders();
    _aiAssignments = _aiAssignments.filter(a => a.provider_id !== provider_id);
    renderAiAssignments();
  } catch (e) { alert(`Delete failed: ${e.message}`); }
}

/* ── Assignment modal ───────────────────────────────────────────────── */

function openAiAssignmentModal(assignment_id) {
  const existing = assignment_id ? _aiAssignments.find(a => a.assignment_id === assignment_id) : null;
  const provOpts = _aiProviders.map(p =>
    `<option value="${esc(p.provider_id)}" ${existing?.provider_id===p.provider_id?'selected':''}>${esc(p.model_name)} — ${esc(p.name)}</option>`
  ).join('');

  const overlay = document.createElement('div');
  overlay.id = 'ai-assignment-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:1000;display:flex;align-items:center;justify-content:center';
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
         padding:24px;width:400px;max-width:95vw">
      <h3 style="margin:0 0 16px">${existing ? 'Edit' : 'Add'} Project Assignment</h3>
      <div style="display:flex;flex-direction:column;gap:10px">
        <label style="font-size:12px;font-weight:600">Project name</label>
        <input id="aia-project" type="text" value="${esc(existing?.project_name || '')}" placeholder="e.g. browser-links" />
        <label style="font-size:12px;font-weight:600">Role</label>
        <select id="aia-role">
          <option value="embedding" ${existing?.role==='embedding'?'selected':''}>embedding</option>
          <option value="reranker"  ${existing?.role==='reranker' ?'selected':''}>reranker</option>
          <option value="llm"       ${existing?.role==='llm'      ?'selected':''}>llm</option>
        </select>
        <label style="font-size:12px;font-weight:600">Provider</label>
        <select id="aia-provider">${provOpts}</select>
        <label style="font-size:12px;font-weight:600">Priority <span style="font-weight:400;color:var(--text-dim)">(higher = preferred)</span></label>
        <input id="aia-priority" type="number" value="${existing?.priority ?? 0}" />
        <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
          <input type="checkbox" id="aia-enabled" ${!existing || existing.enabled ? 'checked' : ''}> Enabled
        </label>
      </div>
      <div style="display:flex;gap:8px;margin-top:18px;justify-content:flex-end">
        <button class="secondary" onclick="document.getElementById('ai-assignment-overlay').remove()">Cancel</button>
        <button onclick="submitAiAssignmentModal('${assignment_id || ''}')">&#128190; Save</button>
      </div>
      <p id="aia-error" class="error-msg" hidden style="margin-top:8px"></p>
    </div>`;
  document.body.appendChild(overlay);
}

async function submitAiAssignmentModal(assignment_id) {
  const project_name = document.getElementById('aia-project').value.trim();
  const role         = document.getElementById('aia-role').value;
  const provider_id  = document.getElementById('aia-provider').value;
  const priority     = parseInt(document.getElementById('aia-priority').value, 10) || 0;
  const enabled      = document.getElementById('aia-enabled').checked;
  const errEl        = document.getElementById('aia-error');

  if (!project_name) { errEl.textContent = 'Project name is required.'; errEl.hidden = false; return; }
  if (!provider_id)  { errEl.textContent = 'Select a provider.'; errEl.hidden = false; return; }

  const body = { project_name, role, provider_id, priority, enabled };
  try {
    const r = await apiFetch(
      assignment_id ? `/api/v1/ai-project-assignments/${encodeURIComponent(assignment_id)}` : '/api/v1/ai-project-assignments',
      { method: assignment_id ? 'PUT' : 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) }
    );
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    document.getElementById('ai-assignment-overlay').remove();
    _aiAssignments = [];
    await loadAiProviders();
  } catch (e) {
    errEl.textContent = `Save failed: ${e.message}`;
    errEl.hidden = false;
  }
}

async function deleteAiAssignment(assignment_id, project, role) {
  if (!confirm(`Remove ${role} assignment for project "${project}"?`)) return;
  try {
    const r = await apiFetch(`/api/v1/ai-project-assignments/${encodeURIComponent(assignment_id)}`, { method: 'DELETE' });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    _aiAssignments = _aiAssignments.filter(a => a.assignment_id !== assignment_id);
    renderAiAssignments();
  } catch (e) { alert(`Delete failed: ${e.message}`); }
}
