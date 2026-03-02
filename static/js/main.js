/**
 * ORA·MIGRATE — Coordinator UI
 * Handles tabs, API polling, rendering, modals, and log.
 */

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

/** Format number with thousands separator */
function fmtNum(n) {
  if (n == null || n === 0) return '0';
  return Number(n).toLocaleString();
}

/** ISO-ish timestamp → HH:MM:SS */
function nowTime() {
  const d = new Date();
  return [d.getHours(), d.getMinutes(), d.getSeconds()]
    .map(v => String(v).padStart(2, '0'))
    .join(':');
}

/** Return HTML for a status dot */
function dotHtml(status) {
  const cls = {
    active:       'dot-green pulsing',
    running:      'dot-amber pulsing',
    completed:    'dot-green',
    connected:    'dot-green pulsing',
    idle:         'dot-amber',
    degraded:     'dot-yellow',
    paused:       'dot-yellow',
    failed:       'dot-red',
    error:        'dot-red',
    disconnected: 'dot-gray',
    cancelled:    'dot-gray',
    pending:      'dot-gray',
  };
  return `<div class="status-dot ${cls[status] ?? 'dot-gray'}"></div>`;
}

/* ═══════════════════════════════════════════════════════════
   Tabs
   ═══════════════════════════════════════════════════════════ */

function initTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const tab = btn.dataset.tab;
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById(`tab-${tab}`).classList.add('active');
    });
  });
}

/* ═══════════════════════════════════════════════════════════
   Renderers — Jobs / Workers / Stats
   ═══════════════════════════════════════════════════════════ */

function renderStats(s) {
  document.getElementById('stat-rows').textContent    = fmtNum(s.total_rows_migrated);
  document.getElementById('stat-active').textContent  = s.active_jobs;
  document.getElementById('stat-active-sub').textContent = `${s.active_jobs} running now`;
  document.getElementById('stat-workers').textContent = s.active_workers;
  document.getElementById('stat-workers-sub').textContent = s.total_throughput;
  document.getElementById('stat-pg').textContent      = s.pg_state_size;

  document.getElementById('h-throughput').textContent = s.total_throughput;
  document.getElementById('h-uptime').textContent     = s.uptime;
  document.getElementById('h-workers').textContent    = `${s.active_workers} active`;
  document.getElementById('h-rows').textContent       = fmtNum(s.total_rows_migrated);
}

function renderJobs(jobs) {
  const grid = document.getElementById('jobs-grid');
  document.getElementById('badge-jobs').textContent = jobs.length;

  if (!jobs.length) {
    grid.innerHTML = `<div style="color:var(--text2);font-family:var(--mono);font-size:12px;padding:32px 0;grid-column:1/-1">No jobs found. Click "+ New Job" to create one.</div>`;
    return;
  }

  grid.innerHTML = jobs.map(j => {
    const failedHtml = j.chunks_failed > 0
      ? `<span class="err">${j.chunks_failed} failed</span>`
      : `${j.chunks_failed} failed`;

    // Для jobs из БД используем source/target из to_dict(), для mock — j.source/j.target
    const sourceLabel = j.source || j.source_conn_id || '—';
    const targetLabel = j.target || j.target_conn_id || '—';
    // Для jobs из БД используем source_table, для mock — j.table
    const tableLabel  = j.source_table || j.table || '—';
    // SCN: из БД — j.scn_cutoff, из mock — j.scn
    const scnLabel    = j.scn_cutoff || j.scn;
    // Throughput: только в mock
    const throughput  = j.throughput || '—';
    // Workers: только в mock
    const workers     = j.workers != null ? j.workers : '—';

    const actions = [];
    if (j.status === 'failed' || j.status === 'cancelled')
                                  actions.push(`<button class="btn btn-ghost btn-sm" onclick="jobAction(${j.id},'retry')">Retry</button>`);
    if (!['completed','cancelled','failed'].includes(j.status))
                                  actions.push(`<button class="btn btn-danger btn-sm" onclick="jobAction(${j.id},'cancel')">Cancel</button>`);
    if (['pending','failed','cancelled'].includes(j.status))
                                  actions.push(`<button class="btn btn-ghost btn-sm" onclick="confirmDeleteJob('${j.id}', '${escHtml(j.name)}')">Delete</button>`);

    return `
    <div class="job-card status-${j.status}" onclick="openJobLogs(${j.id})">
      <div class="jc-head">
        ${dotHtml(j.status)}
        <span class="jc-name">${escHtml(j.name)}</span>
        <span class="jc-status ${j.status}">${j.status}</span>
      </div>
      <div class="jc-body">
        <div class="jc-metrics">
          <div>
            <div class="jc-metric-label">Rows</div>
            <div class="jc-metric-value amber">${fmtNum(j.rows_migrated)}</div>
          </div>
          <div>
            <div class="jc-metric-label">Chunks</div>
            <div class="jc-metric-value">${j.chunks_done}<span class="jc-metric-dim">/${j.chunks_total}</span></div>
          </div>
          <div>
            <div class="jc-metric-label">Workers</div>
            <div class="jc-metric-value">${workers}</div>
          </div>
          <div>
            <div class="jc-metric-label">Throughput</div>
            <div class="jc-metric-value">${throughput}</div>
          </div>
        </div>
        <div class="progress-wrap">
          <div class="progress-label">
            <span class="progress-source">${escHtml(sourceLabel)} → ${escHtml(targetLabel)} · <span style="color:var(--text1)">${escHtml(tableLabel)}</span></span>
            <span class="progress-pct">${j.progress}%</span>
          </div>
          <div class="progress-track">
            <div class="progress-fill ${j.status}" style="width:${j.progress}%"></div>
          </div>
        </div>
      </div>
      <div class="jc-footer">
        <div class="jc-footer-meta">
          <span>${scnLabel ? `SCN <span class="val">${scnLabel}</span>` : 'No SCN'}</span>
          <span class="sep">·</span>
          <span>${j.started_at ? `<span class="val">${fmtDate(j.started_at)}</span>` : 'Not started'}</span>
          <span class="sep">·</span>
          <span>${failedHtml}</span>
        </div>
        <div class="btn-group" onclick="event.stopPropagation()">
          ${actions.join('')}
        </div>
      </div>
    </div>`;
  }).join('');
}

function renderWorkers(workers) {
  const tbody = document.getElementById('workers-tbody');
  document.getElementById('badge-workers').textContent = workers.length;

  tbody.innerHTML = workers.map(w => {
    const hot = w.cpu > 70;
    const staleHb = parseInt(w.heartbeat) > 30;
    return `
    <tr>
      <td class="worker-id">${w.id}</td>
      <td class="worker-name">${w.container}</td>
      <td><span class="worker-status">${dotHtml(w.status)} ${w.status}</span></td>
      <td class="td-dim">
        ${w.job
          ? `<span class="td-amber">${w.job}</span> · <span>${w.chunk ?? '—'}</span>`
          : '—'}
      </td>
      <td>
        <span class="cpu-bar"><span class="cpu-fill${hot ? ' hot' : ''}" style="width:${w.cpu}%"></span></span>
        <span class="${hot ? 'td-red' : 'td-dim'}">${w.cpu}%</span>
      </td>
      <td class="td-dim">${w.mem}</td>
      <td class="${w.rows_s > 0 ? 'td-cyan' : 'td-dim'}">${w.rows_s > 0 ? fmtNum(w.rows_s) : '—'}</td>
      <td class="td-dim">${w.uptime}</td>
      <td class="${staleHb ? 'td-red' : 'td-dim'}">${w.heartbeat}</td>
      <td><button class="btn btn-ghost btn-sm">Inspect</button></td>
    </tr>`;
  }).join('');
}

/* ═══════════════════════════════════════════════════════════
   Connections — Data Loading
   ═══════════════════════════════════════════════════════════ */

async function loadDbConnections() {
  try {
    const r = await fetch('/api/connections/db');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    renderDbConnections(Array.isArray(data) ? data : []);
  } catch (e) {
    renderDbConnections([]);
    addLog('warn', `DB connections unavailable: ${e.message}`);
  }
}

async function loadKafkaConnections() {
  try {
    const r = await fetch('/api/connections/kafka');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    renderKafkaConnections(Array.isArray(data) ? data : []);
  } catch (e) {
    renderKafkaConnections([]);
  }
}

async function loadKafkaConnectConnections() {
  try {
    const r = await fetch('/api/connections/kafka-connect');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    renderKafkaConnectConnections(Array.isArray(data) ? data : []);
  } catch (e) {
    renderKafkaConnectConnections([]);
  }
}

function loadConnections() {
  loadDbConnections();
  loadKafkaConnections();
  loadKafkaConnectConnections();
  // Also update badge via legacy endpoint
  fetch('/api/connections')
    .then(r => r.json())
    .then(data => {
      document.getElementById('badge-conn').textContent = Array.isArray(data) ? data.length : 0;
    })
    .catch(() => {});
}

/* ═══════════════════════════════════════════════════════════
   Connections — Renderers
   ═══════════════════════════════════════════════════════════ */

function renderDbConnections(conns) {
  const grid = document.getElementById('db-conn-grid');
  const empty = document.getElementById('db-conn-empty');

  if (!conns.length) {
    grid.innerHTML = '';
    grid.appendChild(empty || _makeEmpty('No database connections configured.'));
    return;
  }

  grid.innerHTML = conns.map(c => {
    const roleClass = c.role === 'SOURCE' ? 'role-source' : 'role-target';
    const roleLabel = c.role || 'SOURCE';
    const typeClass = (c.db_type || '').toLowerCase() === 'oracle' ? 'oracle' : 'postgresql';
    const typeLabel = typeClass === 'oracle' ? 'Oracle' : 'PostgreSQL';

    return `
    <div class="conn-card">
      <div class="conn-head">
        <span class="conn-role-badge ${roleClass}">${roleLabel}</span>
        <span class="conn-type-badge ${typeClass}">${typeLabel}</span>
        <span class="conn-name">${escHtml(c.name)}</span>
        ${dotHtml(c.status)}
      </div>
      <div class="conn-rows">
        <div class="conn-row">
          <span class="conn-row-label">Host</span>
          <span class="conn-row-val">${escHtml(c.host)}:${c.port}</span>
        </div>
        <div class="conn-row">
          <span class="conn-row-label">${typeClass === 'oracle' ? (c.service_name ? 'Service' : 'SID') : 'Database'}</span>
          <span class="conn-row-val">${escHtml(c.service_name || c.database)}</span>
        </div>
        ${typeClass === 'oracle' && c.service_name && c.database ? `
        <div class="conn-row">
          <span class="conn-row-label">SID</span>
          <span class="conn-row-val">${escHtml(c.database)}</span>
        </div>` : ''}
        <div class="conn-row">
          <span class="conn-row-label">User</span>
          <span class="conn-row-val">${escHtml(c.username)}</span>
        </div>
        <div class="conn-row">
          <span class="conn-row-label">Status</span>
          <span class="conn-status-text ${c.status}">${c.status}</span>
        </div>
        ${c.last_tested_at ? `
        <div class="conn-row">
          <span class="conn-row-label">Last tested</span>
          <span class="conn-row-val">${fmtDate(c.last_tested_at)}</span>
        </div>` : ''}
      </div>
      <div class="conn-footer">
        <button class="btn btn-ghost btn-sm" onclick="testDbConn(${c.id}, this)">Test</button>
        <button class="btn btn-ghost btn-sm" onclick="openEditDbModal(${c.id})">Edit</button>
        <button class="btn btn-danger btn-sm" onclick="confirmDeleteConn('db', ${c.id}, '${escHtml(c.name)}')">Delete</button>
      </div>
    </div>`;
  }).join('');
}

function renderKafkaConnections(conns) {
  const grid = document.getElementById('kafka-conn-grid');

  if (!conns.length) {
    grid.innerHTML = '<div class="conn-empty">No Kafka connections configured.</div>';
    return;
  }

  grid.innerHTML = conns.map(c => `
    <div class="conn-card">
      <div class="conn-head">
        <span class="conn-kafka-badge">KAFKA</span>
        <span class="conn-name">${escHtml(c.name)}</span>
        ${dotHtml(c.status)}
      </div>
      <div class="conn-rows">
        <div class="conn-row">
          <span class="conn-row-label">Bootstrap</span>
          <span class="conn-row-val conn-row-val-wrap">${escHtml(c.bootstrap_servers)}</span>
        </div>
        <div class="conn-row">
          <span class="conn-row-label">Protocol</span>
          <span class="conn-row-val">${escHtml(c.security_protocol)}</span>
        </div>
        ${c.sasl_mechanism ? `
        <div class="conn-row">
          <span class="conn-row-label">SASL</span>
          <span class="conn-row-val">${escHtml(c.sasl_mechanism)}</span>
        </div>` : ''}
        <div class="conn-row">
          <span class="conn-row-label">Status</span>
          <span class="conn-status-text ${c.status}">${c.status}</span>
        </div>
        ${c.last_tested_at ? `
        <div class="conn-row">
          <span class="conn-row-label">Last tested</span>
          <span class="conn-row-val">${fmtDate(c.last_tested_at)}</span>
        </div>` : ''}
      </div>
      <div class="conn-footer">
        <button class="btn btn-ghost btn-sm" onclick="testKafkaConn(${c.id}, this)">Test</button>
        <button class="btn btn-ghost btn-sm" onclick="openEditKafkaModal(${c.id})">Edit</button>
        <button class="btn btn-danger btn-sm" onclick="confirmDeleteConn('kafka', ${c.id}, '${escHtml(c.name)}')">Delete</button>
      </div>
    </div>`).join('');
}

function renderKafkaConnectConnections(conns) {
  const grid = document.getElementById('kafka-connect-grid');

  if (!conns.length) {
    grid.innerHTML = '<div class="conn-empty">No Kafka Connect connections configured.</div>';
    return;
  }

  grid.innerHTML = conns.map(c => `
    <div class="conn-card">
      <div class="conn-head">
        <span class="conn-kafka-connect-badge">KAFKA CONNECT</span>
        <span class="conn-name">${escHtml(c.name)}</span>
        ${dotHtml(c.status)}
      </div>
      <div class="conn-rows">
        <div class="conn-row">
          <span class="conn-row-label">URL</span>
          <span class="conn-row-val conn-row-val-wrap">${escHtml(c.url)}</span>
        </div>
        ${c.username ? `
        <div class="conn-row">
          <span class="conn-row-label">User</span>
          <span class="conn-row-val">${escHtml(c.username)}</span>
        </div>` : ''}
        <div class="conn-row">
          <span class="conn-row-label">Status</span>
          <span class="conn-status-text ${c.status}">${c.status}</span>
        </div>
        ${c.last_tested_at ? `
        <div class="conn-row">
          <span class="conn-row-label">Last tested</span>
          <span class="conn-row-val">${fmtDate(c.last_tested_at)}</span>
        </div>` : ''}
      </div>
      <div class="conn-footer">
        <button class="btn btn-ghost btn-sm" onclick="testKafkaConnectConn(${c.id}, this)">Test</button>
        <button class="btn btn-ghost btn-sm" onclick="openEditKafkaConnectModal(${c.id})">Edit</button>
        <button class="btn btn-danger btn-sm" onclick="confirmDeleteConn('kafka-connect', ${c.id}, '${escHtml(c.name)}')">Delete</button>
      </div>
    </div>`).join('');
}

/* ═══════════════════════════════════════════════════════════
   Connections — Modal Management
   ═══════════════════════════════════════════════════════════ */

function openModal(id) {
  document.getElementById(id).classList.add('open');
}

function closeModal(id) {
  document.getElementById(id).classList.remove('open');
}

/** DB Connection Modal */
function openAddDbModal(role) {
  _resetDbForm();
  document.getElementById('modal-db-title').textContent = 'Add Database Connection';
  document.getElementById('db-conn-id').value = '';
  document.getElementById('db-role').value = role || 'SOURCE';
  onDbTypeChange();
  clearTestResult('db-test-result');
  openModal('modal-db-conn');
}

async function openEditDbModal(id) {
  try {
    const r = await fetch(`/api/connections/db`);
    const list = await r.json();
    const conn = list.find(c => c.id === id);
    if (!conn) return;

    document.getElementById('modal-db-title').textContent = 'Edit Database Connection';
    document.getElementById('db-conn-id').value = conn.id;
    document.getElementById('db-name').value = conn.name;
    document.getElementById('db-role').value = conn.role || 'SOURCE';
    document.getElementById('db-type').value = conn.db_type || 'oracle';
    document.getElementById('db-host').value = conn.host;
    document.getElementById('db-port').value = conn.port;
    document.getElementById('db-database').value = conn.database;
    document.getElementById('db-service-name').value = conn.service_name || '';
    document.getElementById('db-username').value = conn.username;
    document.getElementById('db-password').value = '';
    document.getElementById('db-ssl-mode').value = conn.ssl_mode || '';
    onDbTypeChange();
    clearTestResult('db-test-result');
    openModal('modal-db-conn');
  } catch (e) {
    addLog('error', `Failed to load connection: ${e.message}`);
  }
}

function onDbTypeChange() {
  const type = document.getElementById('db-type').value;
  const portEl = document.getElementById('db-port');
  const sslRow = document.getElementById('db-ssl-mode-row');
  const svcField = document.getElementById('db-service-name-field');
  const dbLabel = document.getElementById('db-database-label');
  const dbHint = document.getElementById('db-database-hint');
  const dbRequired = document.getElementById('db-database-required');
  const dbInput = document.getElementById('db-database');

  if (type === 'oracle') {
    if (portEl.value === '5432') portEl.value = '1521';
    sslRow.style.display = 'none';
    svcField.style.display = '';
    // Для Oracle SID необязателен — достаточно Service Name
    dbInput.placeholder = 'ORCL';
    if (dbHint) dbHint.textContent = '(Oracle, optional if Service Name set)';
    if (dbRequired) dbRequired.style.display = 'none';
    dbInput.removeAttribute('required');
  } else {
    if (portEl.value === '1521') portEl.value = '5432';
    sslRow.style.display = '';
    svcField.style.display = 'none';
    // Для PostgreSQL database обязателен
    dbInput.placeholder = 'mydb';
    if (dbHint) dbHint.textContent = '';
    if (dbRequired) dbRequired.style.display = '';
    dbInput.setAttribute('required', 'required');
  }
}

function _resetDbForm() {
  ['db-name','db-host','db-database','db-service-name','db-username','db-password'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
  document.getElementById('db-port').value = '1521';
  document.getElementById('db-type').value = 'oracle';
  document.getElementById('db-role').value = 'SOURCE';
  document.getElementById('db-ssl-mode').value = '';
}

async function saveDbConnection() {
  const id = document.getElementById('db-conn-id').value;
  const password = document.getElementById('db-password').value;

  const payload = {
    name:         document.getElementById('db-name').value.trim(),
    role:         document.getElementById('db-role').value,
    db_type:      document.getElementById('db-type').value,
    host:         document.getElementById('db-host').value.trim(),
    port:         parseInt(document.getElementById('db-port').value),
    database:     document.getElementById('db-database').value.trim(),
    service_name: document.getElementById('db-service-name').value.trim() || null,
    username:     document.getElementById('db-username').value.trim(),
    ssl_mode:     document.getElementById('db-ssl-mode').value || null,
  };

  if (password) payload.password = password;

  // Валидация: для Oracle нужен хотя бы SID или Service Name
  const isOracle = payload.db_type === 'oracle';
  if (!payload.name || !payload.host || !payload.username) {
    showTestResult('db-test-result', 'error', 'Please fill in all required fields.');
    return;
  }
  if (!isOracle && !payload.database) {
    showTestResult('db-test-result', 'error', 'Database name is required for PostgreSQL.');
    return;
  }
  if (isOracle && !payload.database && !payload.service_name) {
    showTestResult('db-test-result', 'error', 'For Oracle specify SID or Service Name (at least one is required).');
    return;
  }
  if (!id && !password) {
    showTestResult('db-test-result', 'error', 'Password is required for new connections.');
    return;
  }

  try {
    const method = id ? 'PUT' : 'POST';
    const url = id ? `/api/connections/db/${id}` : '/api/connections/db';
    const r = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    closeModal('modal-db-conn');
    addLog('success', `DB connection ${id ? 'updated' : 'created'}: ${payload.name}`);
    loadDbConnections();
    loadConnections();
  } catch (e) {
    showTestResult('db-test-result', 'error', e.message);
  }
}

async function testDbConnectionModal() {
  const id = document.getElementById('db-conn-id').value;
  if (!id) {
    showTestResult('db-test-result', 'warn', 'Save the connection first to test it.');
    return;
  }
  showTestResult('db-test-result', 'info', 'Testing…');
  try {
    const r = await fetch(`/api/connections/db/${id}/test`, { method: 'POST' });
    const data = await r.json();
    showTestResult('db-test-result', data.status === 'connected' ? 'success' : 'error',
      data.message || data.status);
    loadDbConnections();
  } catch (e) {
    showTestResult('db-test-result', 'error', e.message);
  }
}

async function testDbConn(id, btn) {
  btn.disabled = true;
  btn.textContent = '…';
  try {
    const r = await fetch(`/api/connections/db/${id}/test`, { method: 'POST' });
    const data = await r.json();
    addLog(data.status === 'connected' ? 'success' : 'error',
      `DB test [id=${id}]: ${data.message}`);
    loadDbConnections();
  } catch (e) {
    addLog('error', `DB test failed: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Test';
  }
}

/** Kafka Modal */
function openAddKafkaModal() {
  _resetKafkaForm();
  document.getElementById('modal-kafka-title').textContent = 'Add Kafka Connection';
  document.getElementById('kafka-conn-id').value = '';
  clearTestResult('kafka-test-result');
  openModal('modal-kafka-conn');
}

async function openEditKafkaModal(id) {
  try {
    const r = await fetch('/api/connections/kafka');
    const list = await r.json();
    const conn = list.find(c => c.id === id);
    if (!conn) return;

    document.getElementById('modal-kafka-title').textContent = 'Edit Kafka Connection';
    document.getElementById('kafka-conn-id').value = conn.id;
    document.getElementById('kafka-name').value = conn.name;
    document.getElementById('kafka-bootstrap').value = conn.bootstrap_servers;
    document.getElementById('kafka-protocol').value = conn.security_protocol || 'PLAINTEXT';
    document.getElementById('kafka-sasl-mechanism').value = conn.sasl_mechanism || 'PLAIN';
    document.getElementById('kafka-sasl-username').value = conn.sasl_username || '';
    document.getElementById('kafka-sasl-password').value = '';
    onKafkaProtocolChange();
    clearTestResult('kafka-test-result');
    openModal('modal-kafka-conn');
  } catch (e) {
    addLog('error', `Failed to load Kafka connection: ${e.message}`);
  }
}

function onKafkaProtocolChange() {
  const proto = document.getElementById('kafka-protocol').value;
  const isSasl = proto.startsWith('SASL');
  document.getElementById('kafka-sasl-mechanism-field').style.display = isSasl ? '' : 'none';
  document.getElementById('kafka-sasl-creds-row').style.display = isSasl ? '' : 'none';
}

function _resetKafkaForm() {
  ['kafka-name','kafka-bootstrap','kafka-sasl-username','kafka-sasl-password'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
  document.getElementById('kafka-protocol').value = 'PLAINTEXT';
  document.getElementById('kafka-sasl-mechanism').value = 'PLAIN';
  onKafkaProtocolChange();
}

async function saveKafkaConnection() {
  const id = document.getElementById('kafka-conn-id').value;
  const proto = document.getElementById('kafka-protocol').value;
  const isSasl = proto.startsWith('SASL');

  const payload = {
    name:              document.getElementById('kafka-name').value.trim(),
    bootstrap_servers: document.getElementById('kafka-bootstrap').value.trim(),
    security_protocol: proto,
  };

  if (isSasl) {
    payload.sasl_mechanism = document.getElementById('kafka-sasl-mechanism').value;
    payload.sasl_username  = document.getElementById('kafka-sasl-username').value.trim() || null;
    const pwd = document.getElementById('kafka-sasl-password').value;
    if (pwd) payload.sasl_password = pwd;
  }

  if (!payload.name || !payload.bootstrap_servers) {
    showTestResult('kafka-test-result', 'error', 'Please fill in all required fields.');
    return;
  }

  try {
    const method = id ? 'PUT' : 'POST';
    const url = id ? `/api/connections/kafka/${id}` : '/api/connections/kafka';
    const r = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    closeModal('modal-kafka-conn');
    addLog('success', `Kafka connection ${id ? 'updated' : 'created'}: ${payload.name}`);
    loadKafkaConnections();
    loadConnections();
  } catch (e) {
    showTestResult('kafka-test-result', 'error', e.message);
  }
}

async function testKafkaConnectionModal() {
  const id = document.getElementById('kafka-conn-id').value;
  if (!id) {
    showTestResult('kafka-test-result', 'warn', 'Save the connection first to test it.');
    return;
  }
  showTestResult('kafka-test-result', 'info', 'Testing…');
  try {
    const r = await fetch(`/api/connections/kafka/${id}/test`, { method: 'POST' });
    const data = await r.json();
    showTestResult('kafka-test-result', data.status === 'connected' ? 'success' : 'error',
      data.message || data.status);
    loadKafkaConnections();
  } catch (e) {
    showTestResult('kafka-test-result', 'error', e.message);
  }
}

async function testKafkaConn(id, btn) {
  btn.disabled = true;
  btn.textContent = '…';
  try {
    const r = await fetch(`/api/connections/kafka/${id}/test`, { method: 'POST' });
    const data = await r.json();
    addLog(data.status === 'connected' ? 'success' : 'error',
      `Kafka test [id=${id}]: ${data.message}`);
    loadKafkaConnections();
  } catch (e) {
    addLog('error', `Kafka test failed: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Test';
  }
}

/** Kafka Connect Modal */
function openAddKafkaConnectModal() {
  _resetKafkaConnectForm();
  document.getElementById('modal-kafka-connect-title').textContent = 'Add Kafka Connect';
  document.getElementById('kafka-connect-id').value = '';
  clearTestResult('kc-test-result');
  openModal('modal-kafka-connect-conn');
}

async function openEditKafkaConnectModal(id) {
  try {
    const r = await fetch('/api/connections/kafka-connect');
    const list = await r.json();
    const conn = list.find(c => c.id === id);
    if (!conn) return;

    document.getElementById('modal-kafka-connect-title').textContent = 'Edit Kafka Connect';
    document.getElementById('kafka-connect-id').value = conn.id;
    document.getElementById('kc-name').value = conn.name;
    document.getElementById('kc-url').value = conn.url;
    document.getElementById('kc-username').value = conn.username || '';
    document.getElementById('kc-password').value = '';
    clearTestResult('kc-test-result');
    openModal('modal-kafka-connect-conn');
  } catch (e) {
    addLog('error', `Failed to load Kafka Connect connection: ${e.message}`);
  }
}

function _resetKafkaConnectForm() {
  ['kc-name','kc-url','kc-username','kc-password'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
}

async function saveKafkaConnectConnection() {
  const id = document.getElementById('kafka-connect-id').value;
  const payload = {
    name:     document.getElementById('kc-name').value.trim(),
    url:      document.getElementById('kc-url').value.trim(),
    username: document.getElementById('kc-username').value.trim() || null,
  };
  const pwd = document.getElementById('kc-password').value;
  if (pwd) payload.password = pwd;

  if (!payload.name || !payload.url) {
    showTestResult('kc-test-result', 'error', 'Please fill in all required fields.');
    return;
  }

  try {
    const method = id ? 'PUT' : 'POST';
    const url = id ? `/api/connections/kafka-connect/${id}` : '/api/connections/kafka-connect';
    const r = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    closeModal('modal-kafka-connect-conn');
    addLog('success', `Kafka Connect ${id ? 'updated' : 'created'}: ${payload.name}`);
    loadKafkaConnectConnections();
    loadConnections();
  } catch (e) {
    showTestResult('kc-test-result', 'error', e.message);
  }
}

async function testKafkaConnectModal() {
  const id = document.getElementById('kafka-connect-id').value;
  if (!id) {
    showTestResult('kc-test-result', 'warn', 'Save the connection first to test it.');
    return;
  }
  showTestResult('kc-test-result', 'info', 'Testing…');
  try {
    const r = await fetch(`/api/connections/kafka-connect/${id}/test`, { method: 'POST' });
    const data = await r.json();
    showTestResult('kc-test-result', data.status === 'connected' ? 'success' : 'error',
      data.message || data.status);
    loadKafkaConnectConnections();
  } catch (e) {
    showTestResult('kc-test-result', 'error', e.message);
  }
}

async function testKafkaConnectConn(id, btn) {
  btn.disabled = true;
  btn.textContent = '…';
  try {
    const r = await fetch(`/api/connections/kafka-connect/${id}/test`, { method: 'POST' });
    const data = await r.json();
    addLog(data.status === 'connected' ? 'success' : 'error',
      `Kafka Connect test [id=${id}]: ${data.message}`);
    loadKafkaConnectConnections();
  } catch (e) {
    addLog('error', `Kafka Connect test failed: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Test';
  }
}

/** Delete confirmation */
function confirmDeleteConn(type, id, name) {
  document.getElementById('confirm-delete-text').textContent =
    `Delete connection "${name}"? This action cannot be undone.`;
  const btn = document.getElementById('confirm-delete-btn');
  btn.onclick = () => deleteConn(type, id, name);
  openModal('modal-confirm-delete');
}

async function deleteConn(type, id, name) {
  closeModal('modal-confirm-delete');
  const urlMap = { db: 'db', kafka: 'kafka', 'kafka-connect': 'kafka-connect' };
  try {
    const r = await fetch(`/api/connections/${urlMap[type]}/${id}`, { method: 'DELETE' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    addLog('success', `Connection deleted: ${name}`);
    loadConnections();
  } catch (e) {
    addLog('error', `Delete failed: ${e.message}`);
  }
}

/* ═══════════════════════════════════════════════════════════
   UI Helpers
   ═══════════════════════════════════════════════════════════ */

function showTestResult(elId, type, msg) {
  const el = document.getElementById(elId);
  if (!el) return;
  const icons = { success: '✓', error: '✗', warn: '⚠', info: '…' };
  el.className = `modal-test-result test-${type}`;
  el.textContent = `${icons[type] || ''} ${msg}`;
  el.style.display = 'flex';
}

function clearTestResult(elId) {
  const el = document.getElementById(elId);
  if (el) { el.textContent = ''; el.style.display = 'none'; }
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function fmtDate(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleString();
  } catch { return iso; }
}

function _makeEmpty(text) {
  const div = document.createElement('div');
  div.className = 'conn-empty';
  div.textContent = text;
  return div;
}

/* ═══════════════════════════════════════════════════════════
   Log
   ═══════════════════════════════════════════════════════════ */

const LOG_MAX = 300;

const LOG_SAMPLES = [
  ['info',    'Chunk assigned to worker_02: ROWID 0900-1000'],
  ['success', 'Chunk 0880-0900 committed — 200,000 rows'],
  ['info',    'SCN checkpoint saved: 1847401230'],
  ['warn',    'worker_07: retry attempt 2/3 for chunk 0181-0200'],
  ['info',    'Heartbeat received from worker_01, worker_02, worker_03, worker_04'],
  ['success', 'LogMiner session started on PROD_DB'],
  ['info',    'DBMS_PARALLEL_EXECUTE: 40 new chunks queued for job_001'],
  ['warn',    'Connection pool exhausted on DWH_DB — waiting 2s'],
  ['error',   'worker_07: ORA-12541 TNS no listener on ARCHIVE_DB:1521'],
  ['success', 'job_002 CUSTOMERS migration completed — 2,100,000 rows'],
  ['info',    'FOR UPDATE SKIP LOCKED: chunk_id=1084 claimed by worker_03'],
  ['info',    'Graceful shutdown signal received — draining workers'],
];

function addLog(type, msg) {
  const panel = document.getElementById('log-panel');
  const line  = document.createElement('div');
  line.className = 'log-line';
  line.innerHTML = `<span class="log-time">${nowTime()}</span><span class="log-msg ${type}">${msg}</span>`;
  panel.appendChild(line);
  panel.scrollTop = panel.scrollHeight;
  while (panel.children.length > LOG_MAX) panel.removeChild(panel.firstChild);
}

function randomLog() {
  const [type, msg] = LOG_SAMPLES[Math.floor(Math.random() * LOG_SAMPLES.length)];
  addLog(type, msg);
}

function openJobLogs(id) {
  addLog('info', `Viewing job: ${id}`);
  document.querySelector('[data-tab="logs"]').click();
}

/* ═══════════════════════════════════════════════════════════
   Job actions
   ═══════════════════════════════════════════════════════════ */

function jobAction(id, action) {
  fetch(`/api/jobs/${id}/action`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action }),
  })
    .then(r => r.json())
    .then(res => {
      const lvl = res.ok ? 'success' : 'error';
      addLog(lvl, `[job ${id}] action='${action}' → status='${res.new_status ?? res.error ?? 'error'}'`);
      loadJobs();
    })
    .catch(e => addLog('error', `jobAction failed: ${e.message}`));
}

/** Подтверждение удаления job */
function confirmDeleteJob(id, name) {
  document.getElementById('confirm-delete-text').textContent =
    `Delete job "${name}"? This action cannot be undone.`;
  const btn = document.getElementById('confirm-delete-btn');
  btn.onclick = () => deleteJob(id, name);
  openModal('modal-confirm-delete');
}

async function deleteJob(id, name) {
  closeModal('modal-confirm-delete');
  try {
    const r = await fetch(`/api/jobs/${id}`, { method: 'DELETE' });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    addLog('success', `Job deleted: ${name}`);
  } catch (e) {
    addLog('error', `Delete job failed: ${e.message}`);
  } finally {
    loadJobs();
  }
}

/* ═══════════════════════════════════════════════════════════
   Create Job Wizard — State & Navigation
   ═══════════════════════════════════════════════════════════ */

/** Глобальное состояние wizard */
const wizardState = {
  currentStep: 1,
  jobData: {},           // данные из шага 1
  preflightResults: null, // результаты шага 2
  targetObjects: null,   // объекты из шага 3
  selectedActions: [],   // выбранные действия шага 3
};

/**
 * Slugify строки для preview имени коннектора.
 * Приводит к нижнему регистру, заменяет не-алфавитные символы на дефис.
 */
function slugify(str) {
  return (str || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 20);
}

/** Обновить preview имени коннектора в шаге 4 */
function updateConnectorPreview() {
  const prefix = document.getElementById('f-topic-prefix')?.value || 'migration';
  const name   = document.getElementById('f-name')?.value || '';
  const schema = document.getElementById('f-source-schema')?.value || '';
  const table  = document.getElementById('f-source-table')?.value || '';
  const suffix = Math.random().toString(36).slice(2, 8).toUpperCase();

  const preview = `${slugify(prefix)}-${slugify(name)}-${slugify(schema)}-${slugify(table)}-${suffix}`;
  const el = document.getElementById('connector-name-preview');
  if (el) el.textContent = preview || '—';
}

/**
 * Перейти к указанному шагу wizard.
 * Обновляет индикатор шагов и показывает нужный контент.
 */
function wizardGoTo(step) {
  const prev = wizardState.currentStep;
  wizardState.currentStep = step;

  // Обновить индикатор шагов
  for (let i = 1; i <= 4; i++) {
    const item = document.getElementById(`wstep-${i}`);
    if (!item) continue;
    item.classList.remove('active', 'done');
    if (i < step) item.classList.add('done');
    else if (i === step) item.classList.add('active');
  }

  // Показать нужный контент
  document.querySelectorAll('.wizard-step-content').forEach(el => el.classList.remove('active'));
  const content = document.getElementById(`wizard-content-${step}`);
  if (content) content.classList.add('active');

  // Обновить кнопки footer
  renderWizardFooter(step);

  // Автоматические действия при входе на шаг
  if (step === 2 && prev !== 2) {
    runPreflight();
  } else if (step === 3 && prev !== 3) {
    loadTargetObjects();
  } else if (step === 4) {
    populateKafkaConnectSelect();
    populateKafkaSelect();
    updateConnectorPreview();
  }
}

/** Рендер кнопок footer в зависимости от шага */
function renderWizardFooter(step) {
  const left  = document.getElementById('wizard-foot-left');
  const right = document.getElementById('wizard-foot-right');
  if (!left || !right) return;

  left.innerHTML = '';
  right.innerHTML = '';

  if (step === 1) {
    left.innerHTML = `<button class="btn btn-ghost" id="modal-cancel">Отмена</button>`;
    right.innerHTML = `<button class="btn btn-amber" onclick="wizardNext()">Далее →</button>`;
  } else if (step === 2) {
    left.innerHTML = `
      <button class="btn btn-ghost" onclick="wizardBack()">← Назад</button>
      <button class="btn btn-ghost btn-sm" onclick="runPreflight()">🔄 Перезапустить проверки</button>
    `;
    right.innerHTML = `
      <button class="btn btn-ghost btn-sm" onclick="wizardGoTo(3)" id="btn-to-objects">→ Управление объектами</button>
      <button class="btn btn-amber" onclick="wizardNext()" id="btn-preflight-next" disabled>Далее →</button>
    `;
  } else if (step === 3) {
    left.innerHTML = `<button class="btn btn-ghost" onclick="wizardBack()">← Назад</button>`;
    right.innerHTML = `<button class="btn btn-amber" onclick="wizardNext()">Далее →</button>`;
  } else if (step === 4) {
    left.innerHTML = `<button class="btn btn-ghost" onclick="wizardBack()">← Назад</button>`;
    right.innerHTML = `
      <button class="btn btn-ghost" onclick="createJobOnly()" id="btn-job-only">Создать только Job</button>
      <button class="btn btn-amber" onclick="createJobWithConnector()" id="btn-job-connector">Создать Job и Коннектор</button>
    `;
  }

  // Переподключить обработчик Cancel
  document.getElementById('modal-cancel')?.addEventListener('click', () => {
    document.getElementById('create-modal').classList.remove('open');
  });
}

/** Переход к следующему шагу с валидацией */
function wizardNext() {
  const step = wizardState.currentStep;

  if (step === 1) {
    // Валидация шага 1
    const name         = document.getElementById('f-name').value.trim();
    const sourceConnId = document.getElementById('f-source').value;
    const targetConnId = document.getElementById('f-target').value;
    const sourceSchema = document.getElementById('f-source-schema').value;
    const sourceTable  = document.getElementById('f-source-table').value;

    if (!name) {
      showTestResult('job-create-error', 'error', 'Job Name is required.');
      return;
    }
    if (!sourceConnId) {
      showTestResult('job-create-error', 'error', 'Source Connection is required.');
      return;
    }
    if (!targetConnId) {
      showTestResult('job-create-error', 'error', 'Target Connection is required.');
      return;
    }
    if (!sourceSchema) {
      showTestResult('job-create-error', 'error', 'Source Schema is required.');
      return;
    }
    if (!sourceTable) {
      showTestResult('job-create-error', 'error', 'Source Table is required.');
      return;
    }

    clearTestResult('job-create-error');

    // Сохранить данные шага 1
    const targetSchema = document.getElementById('f-target-schema').value;
    const targetTable  = document.getElementById('f-target-table').value;
    const sourceTableFull = `${sourceSchema}.${sourceTable}`;
    let targetTableFull = null;
    if (targetSchema && targetTable) {
      targetTableFull = `${targetSchema}.${targetTable}`;
    } else if (targetSchema) {
      targetTableFull = `${targetSchema}.${sourceTable}`;
    }

    wizardState.jobData = {
      name,
      source_conn_id:  parseInt(sourceConnId),
      target_conn_id:  parseInt(targetConnId),
      source_table:    sourceTableFull,
      target_table:    targetTableFull,
      chunk_size:      parseInt(document.getElementById('f-chunk').value) || 10000,
      migration_mode:  document.getElementById('f-mode').value,
      chunk_strategy:  document.getElementById('f-strategy').value,
      filter_clause:   document.getElementById('f-filter').value.trim() || null,
    };

    wizardGoTo(2);
  } else if (step === 2) {
    wizardGoTo(3);
  } else if (step === 3) {
    wizardGoTo(4);
  }
}

/** Переход к предыдущему шагу */
function wizardBack() {
  if (wizardState.currentStep > 1) {
    wizardGoTo(wizardState.currentStep - 1);
  }
}

/* ═══════════════════════════════════════════════════════════
   Wizard Step 2 — Pre-flight Checks
   ═══════════════════════════════════════════════════════════ */

/**
 * Запустить pre-flight проверки.
 * POST /api/jobs/preflight с данными из шага 1.
 */
async function runPreflight() {
  const loading = document.getElementById('preflight-loading');
  const results = document.getElementById('preflight-results');
  const empty   = document.getElementById('preflight-empty');
  const nextBtn = document.getElementById('btn-preflight-next');

  if (loading) loading.style.display = 'flex';
  if (results) results.style.display = 'none';
  if (empty)   empty.style.display   = 'none';
  if (nextBtn) nextBtn.disabled = true;

  try {
    // Разбираем source_table и target_table на schema + table для API preflight
    const jd = wizardState.jobData;
    const srcParts = (jd.source_table || '').split('.');
    const tgtParts = (jd.target_table || jd.source_table || '').split('.');

    const preflightPayload = {
      job_name:       jd.name || '',
      source_conn_id: jd.source_conn_id,
      target_conn_id: jd.target_conn_id,
      source_schema:  srcParts[0] || '',
      source_table:   srcParts[1] || srcParts[0] || '',
      target_schema:  tgtParts[0] || srcParts[0] || '',
      target_table:   tgtParts[1] || tgtParts[0] || srcParts[1] || srcParts[0] || '',
    };

    const r = await fetch('/api/jobs/preflight', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(preflightPayload),
    });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    wizardState.preflightResults = data;
    renderPreflightResults(data);
  } catch (e) {
    addLog('warn', `Preflight failed: ${e.message}`);
    if (empty) {
      empty.style.display = 'block';
      empty.textContent = `Ошибка при выполнении проверок: ${e.message}`;
    }
    // При ошибке API — разрешаем продолжить
    if (nextBtn) nextBtn.disabled = false;
  } finally {
    if (loading) loading.style.display = 'none';
  }
}

/**
 * Отрендерить результаты pre-flight проверок.
 * @param {Object} data — ответ от /api/jobs/preflight
 */
function renderPreflightResults(data) {
  const results = document.getElementById('preflight-results');
  const empty   = document.getElementById('preflight-empty');
  const tbody   = document.getElementById('preflight-tbody');
  const summary = document.getElementById('preflight-summary');
  const notice  = document.getElementById('preflight-notice');
  const nextBtn = document.getElementById('btn-preflight-next');

  if (!results || !tbody) return;

  const checks = data.results || data.checks || [];
  let okCount = 0, warnCount = 0, errCount = 0;

  tbody.innerHTML = checks.map(c => {
    const status = (c.status || '').toLowerCase();
    let rowClass = 'check-result-ok';
    let statusHtml = `<span class="check-status-ok">✅ OK</span>`;

    if (status === 'warning' || status === 'warn') {
      rowClass = 'check-result-warning';
      statusHtml = `<span class="check-status-warn">⚠️ WARN</span>`;
      warnCount++;
    } else if (status === 'error') {
      rowClass = 'check-result-error';
      statusHtml = `<span class="check-status-error">❌ ERROR</span>`;
      errCount++;
    } else if (status === 'exception') {
      rowClass = 'check-result-exception';
      statusHtml = `<span class="check-status-error">💥 EXCEPTION</span>`;
      errCount++;
    } else {
      okCount++;
    }

    return `
    <tr class="${rowClass}">
      <td>${escHtml(c.name || c.check || '')}</td>
      <td>${statusHtml}</td>
      <td style="color:var(--text1);white-space:normal">${escHtml(c.details || c.message || '')}</td>
    </tr>`;
  }).join('');

  // Summary
  if (summary) {
    summary.innerHTML = `
      <span class="preflight-summary-ok">✅ ${okCount} OK</span>
      <span class="preflight-summary-warn">⚠️ ${warnCount} WARNING</span>
      <span class="preflight-summary-error">❌ ${errCount} ERROR</span>
    `;
  }

  // Notice + кнопка Next
  if (notice) {
    if (errCount > 0) {
      notice.className = 'preflight-notice notice-error';
      notice.textContent = '❌ Исправьте ошибки перед продолжением. Перейдите к шагу 3 для управления объектами.';
      notice.style.display = 'block';
      if (nextBtn) nextBtn.disabled = true;
    } else if (warnCount > 0) {
      notice.className = 'preflight-notice notice-warn';
      notice.textContent = '⚠️ Есть предупреждения. Рекомендуется исправить перед миграцией.';
      notice.style.display = 'block';
      if (nextBtn) nextBtn.disabled = false;
    } else {
      notice.className = 'preflight-notice notice-ok';
      notice.textContent = '✅ Все проверки пройдены успешно.';
      notice.style.display = 'block';
      if (nextBtn) nextBtn.disabled = false;
    }
  }

  if (empty)   empty.style.display   = 'none';
  if (results) results.style.display = 'block';
}

/* ═══════════════════════════════════════════════════════════
   Wizard Step 3 — Target Objects Management
   ═══════════════════════════════════════════════════════════ */

/**
 * Загрузить объекты target (индексы, констрейнты, триггеры).
 * GET /api/jobs/target-objects?conn_id=...&schema=...&table=...
 */
async function loadTargetObjects() {
  const loading = document.getElementById('objects-loading');
  const content = document.getElementById('objects-content');
  const empty   = document.getElementById('objects-empty');

  if (loading) loading.style.display = 'flex';
  if (content) content.style.display = 'none';
  if (empty)   empty.style.display   = 'none';

  const jd = wizardState.jobData;
  if (!jd.target_conn_id) {
    if (empty) {
      empty.style.display = 'block';
      empty.textContent = 'Не выбран target connection. Вернитесь к шагу 1.';
    }
    if (loading) loading.style.display = 'none';
    return;
  }

  // Разбить target_table на schema.table
  const targetParts = (jd.target_table || jd.source_table || '').split('.');
  const schema = targetParts[0] || '';
  const table  = targetParts[1] || '';

  try {
    const params = new URLSearchParams({
      conn_id: jd.target_conn_id,
      schema,
      table,
    });
    const r = await fetch(`/api/jobs/target-objects?${params}`);
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    wizardState.targetObjects = data;
    renderTargetObjects(data);
  } catch (e) {
    addLog('warn', `Target objects load failed: ${e.message}`);
    if (empty) {
      empty.style.display = 'block';
      empty.textContent = `Ошибка загрузки объектов: ${e.message}. Можно продолжить без управления объектами.`;
    }
  } finally {
    if (loading) loading.style.display = 'none';
  }
}

/**
 * Отрендерить объекты target в таблицах.
 * Автоматически отмечает чекбоксы для рекомендуемых действий.
 */
function renderTargetObjects(data) {
  const content = document.getElementById('objects-content');
  const empty   = document.getElementById('objects-empty');

  const indexes     = data.indexes     || [];
  const constraints = data.constraints || [];
  const triggers    = data.triggers    || [];

  // Индексы
  const idxCount = document.getElementById('obj-count-indexes');
  const idxTbody = document.getElementById('obj-tbody-indexes');
  if (idxCount) idxCount.textContent = indexes.length;
  if (idxTbody) {
    if (!indexes.length) {
      idxTbody.innerHTML = `<tr><td colspan="6" style="color:var(--text2);text-align:center">Нет индексов</td></tr>`;
    } else {
      idxTbody.innerHTML = indexes.map(idx => {
        const isPk = idx.uniqueness === 'UNIQUE' && (idx.name || '').startsWith('PK_');
        const status = (idx.status || '').toUpperCase();
        const statusClass = status === 'VALID' || status === 'USABLE' ? 'obj-status-valid' : 'obj-status-unusable';
        // Автовыбор: не-PK индексы со статусом VALID/USABLE
        const autoCheck = !isPk && (status === 'VALID' || status === 'USABLE');
        const action = status === 'VALID' || status === 'USABLE' ? 'UNUSABLE' : 'REBUILD';
        const actionLabel = action === 'UNUSABLE' ? 'Сделать UNUSABLE' : 'Rebuild';

        return `
        <tr>
          <td><input type="checkbox" class="obj-check" data-type="index" data-name="${escHtml(idx.name)}" data-action="${action}" ${autoCheck ? 'checked' : ''} ${isPk ? 'disabled' : ''}></td>
          <td class="obj-name">${escHtml(idx.name)}</td>
          <td class="obj-type">${escHtml(idx.index_type || 'NORMAL')}</td>
          <td class="obj-type">${escHtml(idx.uniqueness || '')}</td>
          <td class="${statusClass}">${escHtml(status)}</td>
          <td>${isPk ? '<span class="obj-pk-note">PK — не трогать</span>' : `<span style="color:var(--text2);font-size:10px">${actionLabel}</span>`}</td>
        </tr>`;
      }).join('');
    }
  }

  // Констрейнты
  const conCount = document.getElementById('obj-count-constraints');
  const conTbody = document.getElementById('obj-tbody-constraints');
  if (conCount) conCount.textContent = constraints.length;
  if (conTbody) {
    if (!constraints.length) {
      conTbody.innerHTML = `<tr><td colspan="5" style="color:var(--text2);text-align:center">Нет констрейнтов</td></tr>`;
    } else {
      conTbody.innerHTML = constraints.map(con => {
        const isPk = (con.type || '').toUpperCase() === 'P';
        const status = (con.status || '').toUpperCase();
        const statusClass = status === 'ENABLED' ? 'obj-status-enabled' : 'obj-status-disabled';
        const typeMap = { P: 'PRIMARY', U: 'UNIQUE', R: 'FK', C: 'CHECK' };
        const typeLabel = con.type_label || typeMap[(con.type || '').toUpperCase()] || con.type;
        // Автовыбор: ENABLED UNIQUE/FK/CHECK (не PK)
        const autoCheck = !isPk && status === 'ENABLED';
        const action = status === 'ENABLED' ? 'DISABLE' : 'ENABLE';
        const actionLabel = action === 'DISABLE' ? 'Отключить' : 'Включить';

        return `
        <tr>
          <td><input type="checkbox" class="obj-check" data-type="constraint" data-name="${escHtml(con.name)}" data-action="${action}" ${autoCheck ? 'checked' : ''} ${isPk ? 'disabled' : ''}></td>
          <td class="obj-name">${escHtml(con.name)}</td>
          <td class="obj-type">${escHtml(typeLabel)}</td>
          <td class="${statusClass}">${escHtml(status)}</td>
          <td>${isPk ? '<span class="obj-pk-note">PK — не трогать</span>' : `<span style="color:var(--text2);font-size:10px">${actionLabel}</span>`}</td>
        </tr>`;
      }).join('');
    }
  }

  // Триггеры
  const trgCount = document.getElementById('obj-count-triggers');
  const trgTbody = document.getElementById('obj-tbody-triggers');
  if (trgCount) trgCount.textContent = triggers.length;
  if (trgTbody) {
    if (!triggers.length) {
      trgTbody.innerHTML = `<tr><td colspan="6" style="color:var(--text2);text-align:center">Нет триггеров</td></tr>`;
    } else {
      trgTbody.innerHTML = triggers.map(trg => {
        const status = (trg.status || '').toUpperCase();
        const statusClass = status === 'ENABLED' ? 'obj-status-enabled' : 'obj-status-disabled';
        // Автовыбор: все ENABLED триггеры
        const autoCheck = status === 'ENABLED';
        const action = status === 'ENABLED' ? 'DISABLE' : 'ENABLE';
        const actionLabel = action === 'DISABLE' ? 'Отключить' : 'Включить';

        return `
        <tr>
          <td><input type="checkbox" class="obj-check" data-type="trigger" data-name="${escHtml(trg.name)}" data-action="${action}" ${autoCheck ? 'checked' : ''}></td>
          <td class="obj-name">${escHtml(trg.name)}</td>
          <td class="obj-type">${escHtml(trg.timing || '')}</td>
          <td class="obj-type">${escHtml(trg.event || '')}</td>
          <td class="${statusClass}">${escHtml(status)}</td>
          <td><span style="color:var(--text2);font-size:10px">${actionLabel}</span></td>
        </tr>`;
      }).join('');
    }
  }

  if (empty)   empty.style.display   = 'none';
  if (content) content.style.display = 'block';
}

/**
 * Применить выбранные действия к объектам target.
 * POST /api/jobs/target-objects/action
 */
async function applySelectedActions() {
  const checkboxes = document.querySelectorAll('.obj-check:checked:not(:disabled)');
  if (!checkboxes.length) {
    const status = document.getElementById('objects-apply-status');
    if (status) status.textContent = 'Нет выбранных объектов.';
    return;
  }

  const targetParts = (wizardState.jobData.target_table || wizardState.jobData.source_table || '').split('.');
  const targetTable = targetParts[1] || '';

  const actions = Array.from(checkboxes).map(cb => {
    const objType  = cb.dataset.type;   // "constraint" | "index" | "trigger"
    const objName  = cb.dataset.name;
    const objAction = (cb.dataset.action || '').toUpperCase(); // "DISABLE" | "ENABLE" | "UNUSABLE" | "REBUILD"

    let actionType;
    if (objType === 'constraint') {
      actionType = objAction === 'DISABLE' ? 'disable_constraint' : 'enable_constraint';
    } else if (objType === 'index') {
      actionType = objAction === 'UNUSABLE' ? 'unusable_index' : 'rebuild_index';
    } else if (objType === 'trigger') {
      actionType = objAction === 'DISABLE' ? 'disable_trigger' : 'enable_trigger';
    } else {
      actionType = objType;
    }

    const item = { type: actionType, name: objName };
    if (objType === 'constraint') {
      item.table = targetTable;
    }
    return item;
  });

  const btn = document.getElementById('btn-apply-actions');
  if (btn) { btn.disabled = true; btn.textContent = 'Применяется…'; }

  const jd = wizardState.jobData;

  try {
    const r = await fetch('/api/jobs/target-objects/action', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        conn_id: jd.target_conn_id,
        schema:  targetParts[0] || '',
        table:   targetParts[1] || '',
        actions,
      }),
    });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    // Показать результаты
    const resultEl = document.getElementById('objects-apply-result');
    if (resultEl) {
      const results = data.results || [];
      resultEl.innerHTML = results.map(res => {
        const ok = res.status === 'ok' || res.status === 'success';
        return `<div class="apply-result-item">
          <span class="${ok ? 'apply-result-ok' : 'apply-result-error'}">${ok ? '✓' : '✗'}</span>
          <span style="color:var(--text1)">${escHtml(res.name)}</span>
          <span style="color:var(--text2)">${escHtml(res.action)}</span>
          ${res.error ? `<span style="color:var(--red)">${escHtml(res.error)}</span>` : ''}
        </div>`;
      }).join('');
      resultEl.classList.add('visible');
    }

    const status = document.getElementById('objects-apply-status');
    if (status) status.textContent = `Применено ${actions.length} действий.`;

    // Перезагрузить объекты
    await loadTargetObjects();
  } catch (e) {
    addLog('error', `Apply actions failed: ${e.message}`);
    const status = document.getElementById('objects-apply-status');
    if (status) status.textContent = `Ошибка: ${e.message}`;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Применить выбранные действия'; }
  }
}

/* ═══════════════════════════════════════════════════════════
   Wizard Step 4 — Create Job & Connector
   ═══════════════════════════════════════════════════════════ */

/** Загрузить Kafka Connect connections в select шага 4 */
async function populateKafkaConnectSelect() {
  const sel = document.getElementById('f-kafka-connect');
  if (!sel) return;

  try {
    const r = await fetch('/api/connections/kafka-connect');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const conns = Array.isArray(data) ? data : [];

    sel.innerHTML = '<option value="">— select Kafka Connect —</option>';
    conns.forEach(c => {
      const opt = document.createElement('option');
      opt.value = String(c.id);
      opt.textContent = `${c.name} (${c.url})`;
      sel.appendChild(opt);
    });

    if (conns.length === 1) {
      sel.value = String(conns[0].id);
    }
  } catch (e) {
    addLog('warn', `Could not load Kafka Connect connections: ${e.message}`);
  }
}

/** Загрузить Kafka broker connections в select шага 4 */
async function populateKafkaSelect() {
  const sel = document.getElementById('f-kafka-broker');
  if (!sel) return;

  try {
    const r = await fetch('/api/connections/kafka');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const conns = Array.isArray(data) ? data : [];

    sel.innerHTML = '<option value="">— select Kafka Broker —</option>';
    conns.forEach(c => {
      const opt = document.createElement('option');
      opt.value = String(c.id);
      opt.textContent = `${c.name} (${c.bootstrap_servers})`;
      sel.appendChild(opt);
    });

    if (conns.length === 1) {
      sel.value = String(conns[0].id);
    }

    if (conns.length === 0) {
      addLog('warn', 'No Kafka broker connections found. Add one in the Connections tab.');
    }
  } catch (e) {
    addLog('warn', `Could not load Kafka broker connections: ${e.message}`);
  }
}

/**
 * Создать Job и Debezium коннектор.
 * POST /api/jobs → POST /api/connectors
 */
async function createJobWithConnector() {
  const btn = document.getElementById('btn-job-connector');
  if (btn) { btn.disabled = true; btn.textContent = 'Создаётся…'; }
  clearTestResult('wizard-create-error');

  try {
    // Шаг 1: создать job
    const jobRes = await _createJob();
    if (!jobRes.ok) throw new Error(jobRes.error || 'Job creation failed');

    const jobId = jobRes.job?.id;
    addLog('success', `Job created: id=${jobId} — ${jobRes.job?.name}`);

    // Шаг 2: создать коннектор
    const kcId = document.getElementById('f-kafka-connect')?.value;
    if (!kcId) {
      showTestResult('wizard-create-error', 'warn', 'Kafka Connect не выбран. Job создан без коннектора.');
      _closeWizardSuccess();
      return;
    }

    const kafkaBrokerEl = document.getElementById('f-kafka-broker');
    const kafkaBrokerId = kafkaBrokerEl ? kafkaBrokerEl.value : '';
    if (!kafkaBrokerId) {
      showTestResult('wizard-create-error', 'error', 'Kafka Broker не выбран. Добавьте Kafka подключение во вкладке Connections.');
      if (btn) { btn.disabled = false; btn.textContent = 'Создать Job и Коннектор'; }
      return;
    }

    const kafkaBrokerIdInt = parseInt(kafkaBrokerId, 10);
    if (isNaN(kafkaBrokerIdInt)) {
      showTestResult('wizard-create-error', 'error', `Некорректный Kafka Broker ID: "${kafkaBrokerId}"`);
      if (btn) { btn.disabled = false; btn.textContent = 'Создать Job и Коннектор'; }
      return;
    }

    const topicPrefix = document.getElementById('f-topic-prefix')?.value || 'migration';
    const scnCutoff   = document.getElementById('f-scn-cutoff')?.value || null;

    const payload = {
      job_id:                jobId,
      kafka_connect_conn_id: parseInt(kcId, 10),
      kafka_conn_id:         kafkaBrokerIdInt,
      source_conn_id:        wizardState.jobData.source_conn_id,
      topic_prefix:          topicPrefix,
      scn_cutoff:            scnCutoff ? parseInt(scnCutoff, 10) : null,
    };

    // Fetch preview
    const previewPayload = { ...payload, preview: true };
    const previewR = await fetch('/api/connectors', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(previewPayload),
    });
    const previewData = await previewR.json();
    if (!previewR.ok) throw new Error(previewData.error || `HTTP ${previewR.status}`);

    // Show modal
    pendingConnectorPayload = payload;
    document.getElementById('preview-connector-config').textContent = JSON.stringify(previewData.config, null, 2);
    openModal('modal-preview-connector');
    
    // Close wizard since job is created
    _closeWizardSuccess();

  } catch (e) {
    showTestResult('wizard-create-error', 'error', e.message);
    addLog('error', `Create job+connector failed: ${e.message}`);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Создать Job и Коннектор'; }
  }
}

let pendingConnectorPayload = null;

async function confirmCreateConnector() {
  const btn = document.getElementById('btn-confirm-connector');
  if (btn) { btn.disabled = true; btn.textContent = 'Creating...'; }
  clearTestResult('preview-connector-error');

  try {
    const connR = await fetch('/api/connectors', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(pendingConnectorPayload),
    });
    const connData = await connR.json();
    if (!connR.ok) throw new Error(connData.error || `HTTP ${connR.status}`);

    addLog('success', `Connector created: ${connData.connector_name}`);
    closeModal('modal-preview-connector');
    loadConnectors(); // Refresh connectors list
  } catch (e) {
    showTestResult('preview-connector-error', 'error', e.message);
    addLog('error', `Create connector failed: ${e.message}`);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Confirm'; }
  }
}

/**
 * Создать только Job (без Debezium коннектора).
 * POST /api/jobs
 */
async function createJobOnly() {
  const btn = document.getElementById('btn-job-only');
  if (btn) { btn.disabled = true; btn.textContent = 'Создаётся…'; }
  clearTestResult('wizard-create-error');

  try {
    const res = await _createJob();
    if (!res.ok) throw new Error(res.error || 'Job creation failed');

    addLog('success', `Job created: id=${res.job?.id} — ${res.job?.name}`);
    _closeWizardSuccess();
  } catch (e) {
    showTestResult('wizard-create-error', 'error', e.message);
    addLog('error', `Create job failed: ${e.message}`);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Создать только Job'; }
  }
}

/** Внутренний хелпер: POST /api/jobs */
async function _createJob() {
  const r = await fetch('/api/jobs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(wizardState.jobData),
  });
  return r.json();
}

/** Закрыть wizard и обновить список jobs */
function _closeWizardSuccess() {
  document.getElementById('create-modal').classList.remove('open');
  loadJobs();
  // Если открыта вкладка connectors — обновить
  const connTab = document.querySelector('[data-tab="connectors"]');
  if (connTab?.classList.contains('active')) loadConnectors();
}

/* ═══════════════════════════════════════════════════════════
   Create Job Modal (legacy initModal → initWizard)
   ═══════════════════════════════════════════════════════════ */

/**
 * Загружает DB connections из API и заполняет selects в форме создания job.
 * Фильтрует: source — только SOURCE role, target — только TARGET role.
 * После загрузки — если есть единственный вариант, выбирает его автоматически.
 */
async function populateJobConnSelects() {
  try {
    const r = await fetch('/api/connections/db');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const conns = await r.json();

    const srcSelect = document.getElementById('f-source');
    const tgtSelect = document.getElementById('f-target');

    srcSelect.innerHTML = '<option value="">— select source DB —</option>';
    tgtSelect.innerHTML = '<option value="">— select target DB —</option>';

    const sources = conns.filter(c => !c.role || c.role === 'SOURCE');
    const targets = conns.filter(c => !c.role || c.role === 'TARGET');

    sources.forEach(c => {
      const opt = document.createElement('option');
      opt.value = c.id;
      opt.textContent = `${c.name} (${(c.db_type || 'oracle').toUpperCase()} · ${c.host}:${c.port})`;
      srcSelect.appendChild(opt);
    });

    targets.forEach(c => {
      const opt = document.createElement('option');
      opt.value = c.id;
      opt.textContent = `${c.name} (${(c.db_type || 'oracle').toUpperCase()} · ${c.host}:${c.port})`;
      tgtSelect.appendChild(opt);
    });

    // Автовыбор если один вариант
    if (sources.length === 1) {
      srcSelect.value = sources[0].id;
      await onSourceConnChange();
    }
    if (targets.length === 1) {
      tgtSelect.value = targets[0].id;
      await onTargetConnChange();
    }

    if (!conns.length) {
      addLog('warn', 'No DB connections configured. Add connections in the Connections tab first.');
    }
  } catch (e) {
    addLog('warn', `Could not load DB connections for job form: ${e.message}`);
  }
}

/**
 * Загрузить схемы для выбранного source connection.
 * Вызывается при изменении f-source select.
 */
async function onSourceConnChange() {
  const connId = document.getElementById('f-source').value;
  const schemaSelect = document.getElementById('f-source-schema');
  const tableSelect  = document.getElementById('f-source-table');

  // Сброс зависимых selects
  schemaSelect.innerHTML = '<option value="">— loading schemas… —</option>';
  schemaSelect.disabled = true;
  tableSelect.innerHTML = '<option value="">— select schema first —</option>';
  tableSelect.disabled = true;

  if (!connId) {
    schemaSelect.innerHTML = '<option value="">— select source first —</option>';
    return;
  }

  try {
    const r = await fetch(`/api/connections/db/${connId}/schemas`);
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    schemaSelect.innerHTML = '<option value="">— select schema —</option>';
    (data.schemas || []).forEach(s => {
      const opt = document.createElement('option');
      opt.value = s;
      opt.textContent = s;
      schemaSelect.appendChild(opt);
    });
    schemaSelect.disabled = false;

    if ((data.schemas || []).length === 0) {
      schemaSelect.innerHTML = '<option value="">No schemas found</option>';
    }
  } catch (e) {
    schemaSelect.innerHTML = `<option value="">Error: ${escHtml(e.message)}</option>`;
    addLog('warn', `Could not load schemas for conn ${connId}: ${e.message}`);
  }
}

/**
 * Загрузить таблицы для выбранной source схемы.
 * Вызывается при изменении f-source-schema select.
 */
async function onSourceSchemaChange() {
  const connId = document.getElementById('f-source').value;
  const schema = document.getElementById('f-source-schema').value;
  const tableSelect = document.getElementById('f-source-table');

  tableSelect.innerHTML = '<option value="">— loading tables… —</option>';
  tableSelect.disabled = true;

  if (!connId || !schema) {
    tableSelect.innerHTML = '<option value="">— select schema first —</option>';
    return;
  }

  try {
    const r = await fetch(`/api/connections/db/${connId}/tables?schema=${encodeURIComponent(schema)}`);
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    tableSelect.innerHTML = '<option value="">— select table —</option>';
    (data.tables || []).forEach(t => {
      const opt = document.createElement('option');
      opt.value = t;
      opt.textContent = t;
      tableSelect.appendChild(opt);
    });
    tableSelect.disabled = false;

    if ((data.tables || []).length === 0) {
      tableSelect.innerHTML = '<option value="">No tables found</option>';
    }
  } catch (e) {
    tableSelect.innerHTML = `<option value="">Error: ${escHtml(e.message)}</option>`;
    addLog('warn', `Could not load tables for ${schema}: ${e.message}`);
  }
}

/**
 * Загрузить схемы для выбранного target connection.
 */
async function onTargetConnChange() {
  const connId = document.getElementById('f-target').value;
  const schemaSelect = document.getElementById('f-target-schema');
  const tableSelect  = document.getElementById('f-target-table');

  schemaSelect.innerHTML = '<option value="">— loading schemas… —</option>';
  schemaSelect.disabled = true;
  tableSelect.innerHTML = '<option value="">— same as source —</option>';
  tableSelect.disabled = true;

  if (!connId) {
    schemaSelect.innerHTML = '<option value="">— select target first —</option>';
    return;
  }

  try {
    const r = await fetch(`/api/connections/db/${connId}/schemas`);
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    schemaSelect.innerHTML = '<option value="">— same as source —</option>';
    (data.schemas || []).forEach(s => {
      const opt = document.createElement('option');
      opt.value = s;
      opt.textContent = s;
      schemaSelect.appendChild(opt);
    });
    schemaSelect.disabled = false;
  } catch (e) {
    schemaSelect.innerHTML = `<option value="">Error: ${escHtml(e.message)}</option>`;
    addLog('warn', `Could not load target schemas for conn ${connId}: ${e.message}`);
  }
}

/**
 * Загрузить таблицы для выбранной target схемы.
 */
async function onTargetSchemaChange() {
  const connId = document.getElementById('f-target').value;
  const schema = document.getElementById('f-target-schema').value;
  const tableSelect = document.getElementById('f-target-table');

  tableSelect.innerHTML = '<option value="">— loading tables… —</option>';
  tableSelect.disabled = true;

  if (!connId || !schema) {
    tableSelect.innerHTML = '<option value="">— same as source —</option>';
    tableSelect.disabled = true;
    return;
  }

  try {
    const r = await fetch(`/api/connections/db/${connId}/tables?schema=${encodeURIComponent(schema)}`);
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    tableSelect.innerHTML = '<option value="">— same as source —</option>';
    (data.tables || []).forEach(t => {
      const opt = document.createElement('option');
      opt.value = t;
      opt.textContent = t;
      tableSelect.appendChild(opt);
    });
    tableSelect.disabled = false;
  } catch (e) {
    tableSelect.innerHTML = `<option value="">Error: ${escHtml(e.message)}</option>`;
    addLog('warn', `Could not load target tables for ${schema}: ${e.message}`);
  }
}

/** Сбросить форму создания job */
function resetJobForm() {
  document.getElementById('f-name').value    = '';
  document.getElementById('f-chunk').value   = '10000';
  document.getElementById('f-mode').value    = 'bulk_cdc';
  document.getElementById('f-strategy').value = 'rowid';
  document.getElementById('f-filter').value  = '';

  // Сброс schema/table selects
  const srcSchema = document.getElementById('f-source-schema');
  const srcTable  = document.getElementById('f-source-table');
  const tgtSchema = document.getElementById('f-target-schema');
  const tgtTable  = document.getElementById('f-target-table');

  srcSchema.innerHTML = '<option value="">— select source first —</option>';
  srcSchema.disabled = true;
  srcTable.innerHTML  = '<option value="">— select schema first —</option>';
  srcTable.disabled = true;
  tgtSchema.innerHTML = '<option value="">— select target first —</option>';
  tgtSchema.disabled = true;
  tgtTable.innerHTML  = '<option value="">— same as source —</option>';
  tgtTable.disabled = true;

  clearTestResult('job-create-error');
}

/**
 * Инициализация wizard создания job.
 * Заменяет старый одношаговый modal.
 */
function initModal() {
  const backdrop = document.getElementById('create-modal');
  const openBtn  = document.getElementById('btn-new-job');
  const closeBtn = document.getElementById('modal-close');

  openBtn?.addEventListener('click', () => {
    // Сброс wizard state
    wizardState.currentStep = 1;
    wizardState.jobData = {};
    wizardState.preflightResults = null;
    wizardState.targetObjects = null;
    wizardState.selectedActions = [];

    resetJobForm();
    populateJobConnSelects();

    // Инициализировать шаг 1
    wizardGoTo(1);
    backdrop.classList.add('open');
  });

  closeBtn?.addEventListener('click', () => backdrop.classList.remove('open'));

  // Каскадная загрузка: при смене source connection — загружаем схемы
  document.getElementById('f-source')?.addEventListener('change', onSourceConnChange);
  // Каскадная загрузка: при смене target connection — загружаем схемы
  document.getElementById('f-target')?.addEventListener('change', onTargetConnChange);

  // Preview коннектора при изменении topic prefix
  document.getElementById('f-topic-prefix')?.addEventListener('input', updateConnectorPreview);

  // Close on backdrop click
  document.querySelectorAll('.modal-backdrop').forEach(bd => {
    bd.addEventListener('click', e => {
      if (e.target === bd) bd.classList.remove('open');
    });
  });

  // Close on Escape
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      document.querySelectorAll('.modal-backdrop.open').forEach(bd => bd.classList.remove('open'));
    }
  });
}

/* ═══════════════════════════════════════════════════════════
   Connectors Tab
   ═══════════════════════════════════════════════════════════ */

/**
 * Загрузить список Debezium коннекторов.
 * GET /api/connectors
 */
async function loadConnectors() {
  try {
    const r = await fetch('/api/connectors');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const connectors = Array.isArray(data) ? data : (data.connectors || []);
    renderConnectors(connectors);
  } catch (e) {
    addLog('warn', `Connectors load failed: ${e.message}`);
    renderConnectors([]);
  }
}

/**
 * Отрендерить таблицу коннекторов.
 * @param {Array} connectors — массив объектов коннекторов
 */
function renderConnectors(connectors) {
  const tbody = document.getElementById('connectors-tbody');
  const badge = document.getElementById('badge-connectors');

  if (badge) badge.textContent = connectors.length || '—';

  if (!tbody) return;

  if (!connectors.length) {
    tbody.innerHTML = `<tr><td colspan="6"><div class="connectors-empty">Нет коннекторов. Создайте job с Debezium коннектором.</div></td></tr>`;
    return;
  }

  tbody.innerHTML = connectors.map(c => {
    const status = (c.status || 'unknown').toLowerCase();
    const statusLabel = status.toUpperCase();
    const statusClass = `connector-status connector-status-${status}`;

    // Кнопки действий в зависимости от статуса
    const actions = [];
    if (status === 'running') {
      actions.push(`<button class="btn btn-ghost btn-sm" onclick="connectorAction(${c.id},'pause')" title="Pause">⏸</button>`);
      actions.push(`<button class="btn btn-ghost btn-sm" onclick="connectorAction(${c.id},'restart')" title="Restart">↺</button>`);
    } else if (status === 'paused') {
      actions.push(`<button class="btn btn-ghost btn-sm" onclick="connectorAction(${c.id},'resume')" title="Resume">▶</button>`);
      actions.push(`<button class="btn btn-ghost btn-sm" onclick="connectorAction(${c.id},'restart')" title="Restart">↺</button>`);
    } else if (status === 'failed') {
      actions.push(`<button class="btn btn-ghost btn-sm" onclick="connectorAction(${c.id},'restart')" title="Restart">↺</button>`);
    }
    if (status !== 'creating') {
      actions.push(`<button class="btn btn-danger btn-sm" onclick="connectorAction(${c.id},'delete')" title="Delete">🗑</button>`);
    }

    const tableLabel = c.source_table || c.table || '—';
    const jobLabel   = c.job_name || c.job_id || '—';
    const createdAt  = c.created_at ? fmtDate(c.created_at) : '—';

    return `
    <tr class="connector-row" onclick="toggleConnectorDetail(${c.id})">
      <td><span class="connector-name" title="${escHtml(c.connector_name)}">${escHtml(c.connector_name)}</span></td>
      <td><span class="${statusClass}">${statusLabel}</span></td>
      <td><span class="connector-job-link">${escHtml(String(jobLabel))}</span></td>
      <td style="color:var(--text1)">${escHtml(tableLabel)}</td>
      <td style="color:var(--text2)">${escHtml(createdAt)}</td>
      <td onclick="event.stopPropagation()">
        <div class="btn-group">${actions.join('')}</div>
      </td>
    </tr>
    <tr class="connector-detail-row" id="connector-detail-row-${c.id}">
      <td colspan="6">
        <div class="connector-row-detail" id="connector-detail-${c.id}">
          ${_renderConnectorDetail(c)}
        </div>
      </td>
    </tr>`;
  }).join('');
}

/**
 * Рендер детальной панели коннектора.
 * @param {Object} c — объект коннектора
 */
function _renderConnectorDetail(c) {
  const config = c.config || {};
  const configHtml = Object.entries(config).map(([k, v]) =>
    `<div><span class="connector-config-key">${escHtml(k)}</span>: <span class="connector-config-value">${escHtml(String(v))}</span></div>`
  ).join('');

  const errorHtml = c.error
    ? `<div class="connector-error-block">Error: ${escHtml(c.error)}</div>`
    : '';

  return `
  <div class="connector-detail-grid">
    <div class="connector-detail-item">
      <div class="connector-detail-label">Connector Name</div>
      <div class="connector-detail-value">${escHtml(c.connector_name)}</div>
    </div>
    <div class="connector-detail-item">
      <div class="connector-detail-label">Job</div>
      <div class="connector-detail-value">${escHtml(String(c.job_name || c.job_id || '—'))} ${c.job_id ? `(ID: ${c.job_id})` : ''}</div>
    </div>
    <div class="connector-detail-item">
      <div class="connector-detail-label">Source Table</div>
      <div class="connector-detail-value">${escHtml(c.source_table || c.table || '—')}</div>
    </div>
    <div class="connector-detail-item">
      <div class="connector-detail-label">SCN Cutoff</div>
      <div class="connector-detail-value">${escHtml(String(c.scn_cutoff || '—'))}</div>
    </div>
    <div class="connector-detail-item">
      <div class="connector-detail-label">Kafka Connect</div>
      <div class="connector-detail-value">${escHtml(c.kafka_connect_url || c.kafka_connect_id || '—')}</div>
    </div>
    <div class="connector-detail-item">
      <div class="connector-detail-label">Last Status Check</div>
      <div class="connector-detail-value">${c.last_checked_at ? fmtDate(c.last_checked_at) : '—'}</div>
    </div>
  </div>
  ${Object.keys(config).length ? `
  <div class="connector-detail-label" style="margin-bottom:6px">Config</div>
  <div class="connector-config-block">${configHtml || '<span style="color:var(--text2)">No config available</span>'}</div>
  ` : ''}
  ${errorHtml}`;
}

/**
 * Переключить видимость детальной панели коннектора.
 * @param {number} id — ID коннектора
 */
function toggleConnectorDetail(id) {
  const detail = document.getElementById(`connector-detail-${id}`);
  if (!detail) return;
  detail.classList.toggle('open');
}

/**
 * Обновить все коннекторы (принудительный refresh через API).
 * GET /api/connectors/refresh-all
 */
async function refreshAllConnectors() {
  try {
    const r = await fetch('/api/connectors/refresh-all');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    addLog('info', `Connectors refreshed: updated=${data.updated ?? 0}, errors=${data.errors?.length ?? 0}`);
  } catch (e) {
    addLog('warn', `Refresh connectors failed: ${e.message}`);
  } finally {
    loadConnectors();
  }
}

/**
 * Выполнить действие над коннектором.
 * POST /api/connectors/{id}/action
 * @param {number} id — ID коннектора
 * @param {string} action — 'pause' | 'resume' | 'restart' | 'delete'
 */
async function connectorAction(id, action) {
  if (action === 'delete') {
    if (!confirm(`Удалить коннектор #${id}? Это действие необратимо.`)) return;
  }

  try {
    const r = await fetch(`/api/connectors/${id}/action`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action }),
    });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);

    addLog('success', `Connector #${id}: action '${action}' → ${data.status || 'ok'}`);
    loadConnectors();
  } catch (e) {
    addLog('error', `Connector action failed: ${e.message}`);
  }
}

/* ═══════════════════════════════════════════════════════════
   Data loading
   ═══════════════════════════════════════════════════════════ */

function loadStats()   { fetch('/api/stats').then(r => r.json()).then(renderStats); }
function loadJobs()    { fetch('/api/jobs').then(r => r.json()).then(renderJobs); }
function loadWorkers() { fetch('/api/workers').then(r => r.json()).then(renderWorkers); }

function loadAll() {
  loadStats();
  loadJobs();
  loadWorkers();
  loadConnections();
}

/* ═══════════════════════════════════════════════════════════
   Init
   ═══════════════════════════════════════════════════════════ */

document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  initModal();

  document.getElementById('btn-clear-log')
    ?.addEventListener('click', () => {
      document.getElementById('log-panel').innerHTML = '';
    });

  // Initial load
  loadAll();
  loadConnectors();

  addLog('success', 'Coordinator UI initialized');
  addLog('info',    'Connected to state DB: localhost:5432/migration_state');
  addLog('info',    'Polling interval: 5 000 ms');

  // Auto-refresh every 5 seconds
  setInterval(() => {
    loadAll();
    randomLog();

    // Обновить коннекторы если активна вкладка Connectors
    const connTab = document.querySelector('[data-tab="connectors"].active');
    if (connTab) loadConnectors();
  }, 5000);
});
