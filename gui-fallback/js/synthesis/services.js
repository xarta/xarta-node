/* ── Services ─────────────────────────────────────────────────────────── */
let _svcFilterTimer = null;  // debounce handle for search-input

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('add-modal-save-btn')?.addEventListener('click', submitAddService);

  const svcSearch = document.getElementById('search-input');
  if (svcSearch) {
    svcSearch.addEventListener('input', () => {
      clearTimeout(_svcFilterTimer);
      _svcFilterTimer = setTimeout(renderServices, 250);
    });
  }
  if (typeof ResponsiveLayout !== 'undefined') {
    ResponsiveLayout.registerTabControls('services', 'pg-ctrl-services');
  }
});
async function loadServices() {
  const err = document.getElementById('services-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/services');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _services = await r.json();
    renderServices();
  } catch (e) {
    err.textContent = `Failed to load services: ${e.message}`;
    err.hidden = false;
  }
}

function renderServices() {
  const q = (document.getElementById('search-input').value || '').toLowerCase();
  const tbody = document.getElementById('services-tbody');
  const visible = _services.filter(s =>
    !q || s.name.toLowerCase().includes(q) ||
    (s.description || '').toLowerCase().includes(q) ||
    (s.service_id || '').toLowerCase().includes(q) ||
    (s.vm_or_lxc || '').toLowerCase().includes(q)
  );
  if (!visible.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="7">' +
      (_services.length ? 'No matching services.' : 'No services — add one above.') + '</td></tr>';
    return;
  }
  tbody.innerHTML = visible.map(s => {
    const tags = (s.tags || []).map(t => `<span class="tag">${esc(t)}</span>`).join('');
    const statusCls = `status-${(s.project_status||'').replace(/[^a-z]/g,'')}`;
    const links = (s.links || []);
    const primaryUrl = links.length ? (links[0].url || links[0].href || '') : '';
    const linkBadges = links.map(l => {
      const href = l.url || l.href || '#';
      const label = l.label || l.name || href;
      return `<a class="link-badge" href="${esc(href)}" target="_blank" rel="noopener">${esc(label)}</a>`;
    }).join('');
    const nameHtml = primaryUrl
      ? `<a class="name-link" href="${esc(primaryUrl)}" target="_blank" rel="noopener"><strong>${esc(s.name)}</strong></a>`
      : `<strong>${esc(s.name)}</strong>`;
    return `<tr>
      <td><code style="font-size:12px;color:var(--text-dim)">${esc(s.service_id)}</code></td>
      <td>${nameHtml}</td>
      <td>${esc(s.host_machine||'')}${s.vm_or_lxc ? ' / '+esc(s.vm_or_lxc) : ''}</td>
      <td><span class="${statusCls}">${esc(s.project_status||'')}</span></td>
      <td>${tags}</td>
      <td>${linkBadges}</td>
      <td style="color:var(--text-dim);font-size:13px">${esc(s.description||'')}</td>
    </tr>`;
  }).join('');
}

/* ── Add service modal ────────────────────────────────────────────────── */
function openAddModal() {
  ['m-id','m-name','m-host','m-lxc','m-desc','m-url'].forEach(id => {
    document.getElementById(id).value = '';
  });
  document.getElementById('modal-error').textContent = '';
  HubModal.open(document.getElementById('add-modal'));
}

async function submitAddService() {
  const id   = document.getElementById('m-id').value.trim();
  const name = document.getElementById('m-name').value.trim();
  const err  = document.getElementById('modal-error');
  if (!id || !name) { err.textContent = 'ID and Name are required'; return; }
  try {
    const r = await apiFetch('/api/v1/services', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        service_id:    id,
        name,
        host_machine:  document.getElementById('m-host').value.trim() || null,
        vm_or_lxc:     document.getElementById('m-lxc').value.trim()  || null,
        description:   document.getElementById('m-desc').value.trim() || null,
        project_status: 'deployed',
        links: (() => {
          const u = document.getElementById('m-url').value.trim();
          return u ? [{ label: 'Open', url: u }] : null;
        })(),
      }),
    });
    if (!r.ok) { const t=await r.text(); throw new Error(t); }
    HubModal.close(document.getElementById('add-modal'));
    await loadServices();
  } catch (e) {
    err.textContent = `Error: ${e.message}`;
  }
}
