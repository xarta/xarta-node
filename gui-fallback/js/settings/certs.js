/* ── Certs ──────────────────────────────────────────────────────────────── */
async function loadCerts() {
  const tbody = document.getElementById('certs-status-tbody');
  const err   = document.getElementById('certs-status-error');
  err.hidden  = true;
  tbody.innerHTML = '<tr class="empty-row"><td colspan="5">Loading…</td></tr>';
  try {
    const r = await apiFetch('/api/v1/certs/status');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    renderCertsTable(data.certs, data.certs_dir);
  } catch (e) {
    err.textContent = `Failed to load cert status: ${e.message}`;
    err.hidden = false;
    tbody.innerHTML = '<tr class="empty-row"><td colspan="5">—</td></tr>';
  }
}

function _expiryStyle(days) {
  if (days === null || days === undefined) return '';
  if (days < 0)  return 'color:var(--err);font-weight:600';
  if (days < 7)  return 'color:var(--err)';
  if (days < 30) return 'color:#e8a82a';
  return 'color:var(--ok)';
}

function _expiryLabel(expires, days) {
  if (!expires) return '';
  const suffix = days !== null && days !== undefined
    ? (days < 0 ? ` (expired ${Math.abs(days)}d ago)` : ` (${days}d)`)
    : '';
  return expires + suffix;
}

function renderCertsTable(certs, certsDir) {
  const tbody = document.getElementById('certs-status-tbody');
  const dirEl = document.getElementById('certs-dir-hint');
  if (dirEl) dirEl.textContent = certsDir || '(not configured)';

  if (!certs || !certs.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="5">No certificate slots configured.</td></tr>';
    return;
  }

  const groups = { caddy: [], mtls: [] };
  certs.forEach(c => (groups[c.group] || groups.caddy).push(c));

  const GROUP_LABELS = { caddy: 'Caddy TLS', mtls: 'mTLS Sync' };
  let html = '';

  for (const [groupKey, groupCerts] of Object.entries(groups)) {
    if (!groupCerts.length) continue;
    html += `<tr style="background:var(--bg)">
      <td colspan="5" style="font-size:11px;font-weight:700;text-transform:uppercase;
        letter-spacing:.6px;color:var(--text-dim);padding:6px 8px;border-bottom:1px solid var(--border)">
        ${esc(GROUP_LABELS[groupKey] || groupKey)}
      </td></tr>`;

    groupCerts.forEach(c => {
      const statusCell = c.present
        ? `<span style="color:var(--ok)">&#10003; present</span>`
        : `<span style="color:var(--err)">&#10007; missing</span>`;

      let detailCell = '';
      if (c.kind === 'key') {
        detailCell = `<span style="color:var(--text-dim);font-size:12px">private key</span>`;
      } else if (c.cn || c.expires) {
        const cnPart  = c.cn ? `<span style="font-family:monospace;font-size:11px">${esc(c.cn)}</span>` : '';
        const expPart = c.expires
          ? `<span style="${_expiryStyle(c.expires_days)};font-size:11px">
               Exp: ${esc(_expiryLabel(c.expires, c.expires_days))}
             </span>`
          : '';
        const caPart  = c.is_ca ? `<span style="color:var(--text-dim);font-size:11px"> [CA]</span>` : '';
        detailCell = [cnPart, expPart + caPart].filter(Boolean).join('<br>');
      }

      const pathNote = c.path_source === 'default'
        ? ` <span title="Env var ${esc(c.env_var)} not set — using default path"
              style="color:var(--text-dim);font-size:10px">(default)</span>` : '';

      html += `<tr>
        <td style="font-weight:600">${esc(c.label)}</td>
        <td style="font-family:monospace;font-size:11px;color:var(--text-dim)">${esc(c.env_var)}</td>
        <td style="font-family:monospace;font-size:11px;color:var(--text-dim);max-width:280px;
          word-break:break-all">${esc(c.path)}${pathNote}</td>
        <td>${statusCell}</td>
        <td>${detailCell}</td>
        <td><button class="secondary" style="padding:2px 8px;font-size:12px"
            onclick="openCertUpload('${esc(c.id)}','${esc(c.label)}','${esc(c.kind)}')">Upload</button></td>
      </tr>`;
    });
  }

  tbody.innerHTML = html;
}

/* ── Upload panel ────────────────────────────────────────────────────────── */
function openCertUpload(id, label, kind) {
  const panel     = document.getElementById('certs-upload-panel');
  const titleEl   = document.getElementById('certs-upload-title');
  const hintEl    = document.getElementById('certs-upload-hint');
  const textarea  = document.getElementById('certs-upload-pem');
  const resultEl  = document.getElementById('certs-upload-result');
  const idInput   = document.getElementById('certs-upload-id');

  idInput.value      = id;
  titleEl.textContent = `Upload: ${label}`;
  textarea.value     = '';
  resultEl.textContent = '';
  resultEl.style.color = '';

  const hints = {
    ca:   'Paste the CA certificate PEM (-----BEGIN CERTIFICATE-----). ' +
          'It will be installed into the system trust store automatically.',
    cert: 'Paste the certificate PEM (-----BEGIN CERTIFICATE-----). ' +
          'Do not include private key material here.',
    key:  'Paste the private key PEM (-----BEGIN ... PRIVATE KEY-----). ' +
          'A corresponding certificate must be uploaded to the matching cert slot.',
  };
  hintEl.textContent = hints[kind] || '';

  panel.style.display = 'block';
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  textarea.focus();
}

function closeCertUpload() {
  document.getElementById('certs-upload-panel').style.display = 'none';
  document.getElementById('certs-upload-pem').value = '';
  document.getElementById('certs-upload-result').textContent = '';
}

/* File-from-disk loader — reads the file and fills the textarea */
function certLoadFile() {
  const fi = document.getElementById('certs-file-input');
  fi.value = '';
  fi.onchange = function () {
    const file = fi.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = function (e) {
      document.getElementById('certs-upload-pem').value = e.target.result;
    };
    reader.onerror = function () {
      document.getElementById('certs-upload-result').textContent = 'Failed to read file.';
      document.getElementById('certs-upload-result').style.color = 'var(--err)';
    };
    reader.readAsText(file);
  };
  fi.click();
}

async function submitCertUpload() {
  const id       = document.getElementById('certs-upload-id').value;
  const pem      = document.getElementById('certs-upload-pem').value.trim();
  const resultEl = document.getElementById('certs-upload-result');
  const btn      = document.getElementById('certs-upload-btn');

  if (!pem) {
    resultEl.textContent = 'Nothing to upload — paste PEM content or load a file first.';
    resultEl.style.color = 'var(--err)';
    return;
  }

  btn.disabled = true;
  resultEl.textContent = 'Uploading…';
  resultEl.style.color = 'var(--text-dim)';

  try {
    const r = await apiFetch('/api/v1/certs/upload', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ id, pem }),
    });
    const data = await r.json();

    if (!r.ok || data.status === 'failed') {
      resultEl.textContent = `Failed: ${data.detail || data.status}`;
      resultEl.style.color = 'var(--err)';
    } else {
      let msg = `Uploaded successfully.`;
      if (data.ca_installed) msg += ` CA: ${data.ca_installed}.`;
      resultEl.textContent = msg;
      resultEl.style.color = 'var(--ok)';
      // Refresh the status table
      loadCerts();
    }
  } catch (e) {
    resultEl.textContent = `Error: ${e.message}`;
    resultEl.style.color = 'var(--err)';
  } finally {
    btn.disabled = false;
  }
}
