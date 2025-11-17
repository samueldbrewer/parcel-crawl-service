// Page detection
const pageName = document.body.dataset.page || 'designs';

// Common elements (status/log/details)
const statusText = document.getElementById('statusText');
const resultBox = document.getElementById('resultBox');
const logTailBox = document.getElementById('logTail');
const jobDetails = document.getElementById('jobDetails');
const historyTableBody = document.querySelector('#historyTable tbody');
const artifactsTableBody = document.querySelector('#artifactsTable tbody');

// Designs page elements
const filesTableBody = document.querySelector('#filesTable tbody');
const refreshBtn = document.getElementById('refreshFiles');
const uploadForm = document.getElementById('uploadForm');
const uploadFileInput = document.getElementById('uploadFile');
const jobForm = document.getElementById('jobForm');
const startButton = document.getElementById('startButton');
const dxfSelect = document.getElementById('dxfSelect');
const designsList = document.getElementById('designsList');
const saveDesignBtn = document.getElementById('saveDesign');
const designNameInput = document.getElementById('designName');
const designPreview = document.getElementById('designPreview');

const captureModal = document.getElementById('captureModal');
const openCaptureBtn = document.getElementById('openCapture');
const closeCaptureBtn = document.getElementById('closeCapture');
const applyCaptureBtn = document.getElementById('applyCapture');
const footprintSummary = document.getElementById('footprintSummary');
const frontSummary = document.getElementById('frontSummary');
const canvas = document.getElementById('captureCanvas');
const ctx = canvas ? canvas.getContext('2d') : null;
const footprintModeBtn = document.getElementById('footprintMode');
const frontModeBtn = document.getElementById('frontMode');
const clearCanvasBtn = document.getElementById('clearCanvas');

// Map page elements
const mapDesignSelect = document.getElementById('mapDesignSelect');
const mapStatus = document.getElementById('mapStatus');
const mapAddressInput = document.getElementById('mapAddress');
const mapProgress = document.getElementById('mapProgress');

// Config elements
const cfgCycles = document.getElementById('cfgCycles');
const cfgBuffer = document.getElementById('cfgBuffer');
const cfgRotation = document.getElementById('cfgRotation');
const cfgScoreWorkers = document.getElementById('cfgScoreWorkers');

// Jobs page elements
const jobsTableBody = document.querySelector('#jobsTable tbody');
const refreshJobsBtn = document.getElementById('refreshJobs');
const jobsDetailBox = document.getElementById('jobsDetailBox');

const padding = 20;
let pollTimer = null;
const jobHistory = new Map();
let currentFilename = null;
let geometryPaths = [];
let viewBox = { minX: -50, maxX: 50, minY: -50, maxY: 50, scale: 2 };
let captureMode = 'footprint';
let rectanglePoints = [];
let frontPoints = [];
let footprintWorld = [];
let frontOrigin = null;
let frontVector = null;
let shrinkwrapReady = false;
let mapInstance = null;
let geoLayer = null;
let selectedDesign = null;

function setStatus(text, loading = false) {
  statusText.textContent = text;
  statusText.classList.toggle('loading', loading);
}

function readyForJob() {
  return shrinkwrapReady && footprintWorld.length >= 3 && !!frontVector;
}

function updateStartButton() {
  if (readyForJob()) {
    startButton.disabled = false;
    startButton.textContent = 'Start Crawl';
  } else {
    startButton.disabled = true;
    startButton.textContent = 'Capture Footprint First';
  }
}

function updateHistory(job) {
  jobHistory.set(job.id, {
    status: job.status,
    updated: new Date().toLocaleTimeString(),
  });
  renderHistory();
}

function renderHistory() {
  historyTableBody.innerHTML = '';
  Array.from(jobHistory.entries())
    .reverse()
    .forEach(([id, info]) => {
      const row = document.createElement('tr');
      const idCell = document.createElement('td');
      idCell.textContent = id;
      const statusCell = document.createElement('td');
      statusCell.textContent = info.status;
      const updatedCell = document.createElement('td');
      updatedCell.textContent = info.updated;
      row.appendChild(idCell);
      row.appendChild(statusCell);
      row.appendChild(updatedCell);
      historyTableBody.appendChild(row);
    });
}

async function refreshFiles() {
  try {
    const resp = await fetch('/files');
    if (!resp.ok) throw new Error('Request failed');
    const data = await resp.json();
    filesTableBody.innerHTML = '';
    dxfSelect.innerHTML = '<option value="">-- Select a file --</option>';
    data.forEach((file) => {
      const row = document.createElement('tr');
      const nameCell = document.createElement('td');
      nameCell.textContent = file.filename;
      const linkCell = document.createElement('td');
      const link = document.createElement('a');
      link.href = file.download_url;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = 'Download';
      linkCell.appendChild(link);
      const deleteCell = document.createElement('td');
      const delBtn = document.createElement('button');
      delBtn.textContent = 'Delete';
      delBtn.className = 'danger';
      delBtn.addEventListener('click', () => deleteFile(file.filename));
      deleteCell.appendChild(delBtn);
      row.appendChild(nameCell);
      row.appendChild(linkCell);
      row.appendChild(deleteCell);
      filesTableBody.appendChild(row);

      const opt = document.createElement('option');
      opt.value = file.file_url;
      opt.textContent = file.filename;
      opt.dataset.filename = file.filename;
      dxfSelect.appendChild(opt);
    });
    setStatus(`Loaded ${data.length} files.`);
  } catch (err) {
    setStatus(`Failed to fetch files: ${err}`);
  }
}

async function deleteFile(filename) {
  if (!window.confirm(`Delete ${filename}?`)) return;
  try {
    const resp = await fetch(`/files/${encodeURIComponent(filename)}`, { method: 'DELETE' });
    if (!resp.ok) throw new Error(await resp.text());
    setStatus(`Deleted ${filename}.`);
    await refreshFiles();
  } catch (err) {
    setStatus(`Delete failed: ${err}`);
  }
}

async function handleUpload(event) {
  event.preventDefault();
  if (!uploadFileInput.files.length) {
    alert('Choose a file first.');
    return;
  }
  const formData = new FormData();
  formData.append('file', uploadFileInput.files[0]);
  setStatus('Uploading…', true);
  try {
    const resp = await fetch('/files', { method: 'POST', body: formData });
    if (!resp.ok) throw new Error(await resp.text());
    const payload = await resp.json();
    resultBox.textContent = JSON.stringify(payload, null, 2);
    setStatus(`Uploaded ${payload.filename}.`, false);
    await refreshFiles();
    Array.from(dxfSelect.options).forEach((opt) => {
      if (opt.dataset.filename === payload.filename) opt.selected = true;
    });
  } catch (err) {
    setStatus(`Upload failed: ${err}`);
  }
}

function worldToCanvas([x, y]) {
  const cx = ((x - viewBox.minX) * viewBox.scale) + padding;
  const cy = canvas.height - (((y - viewBox.minY) * viewBox.scale) + padding);
  return [cx, cy];
}

function canvasToWorld(x, y) {
  const wx = ((x - padding) / viewBox.scale) + viewBox.minX;
  const wy = ((canvas.height - y - padding) / viewBox.scale) + viewBox.minY;
  return [wx, wy];
}

function recomputeViewBox() {
  const xs = [];
  const ys = [];
  geometryPaths.forEach((path) => {
    path.forEach(([x, y]) => {
      xs.push(x);
      ys.push(y);
    });
  });
  footprintWorld.forEach(([x, y]) => {
    xs.push(x);
    ys.push(y);
  });
  if (frontOrigin) {
    xs.push(frontOrigin[0]);
    ys.push(frontOrigin[1]);
  }
  if (frontOrigin && frontVector) {
    xs.push(frontOrigin[0] + frontVector[0]);
    ys.push(frontOrigin[1] + frontVector[1]);
  }
  if (!xs.length) {
    viewBox = { minX: -50, maxX: 50, minY: -50, maxY: 50, scale: 2 };
    return;
  }
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const rangeX = Math.max(1, maxX - minX);
  const rangeY = Math.max(1, maxY - minY);
  const scale = Math.min(
    (canvas.width - padding * 2) / rangeX,
    (canvas.height - padding * 2) / rangeY,
  );
  viewBox = { minX, maxX, minY, maxY, scale };
}

function drawGeometry() {
  ctx.strokeStyle = '#cbd5f5';
  ctx.lineWidth = 1;
  geometryPaths.forEach((path) => {
    if (!path.length) return;
    ctx.beginPath();
    const [sx, sy] = worldToCanvas(path[0]);
    ctx.moveTo(sx, sy);
    for (let i = 1; i < path.length; i += 1) {
      const [cx, cy] = worldToCanvas(path[i]);
      ctx.lineTo(cx, cy);
    }
    ctx.stroke();
  });
}

function drawRectanglePreview() {
  if (!rectanglePoints.length) return;
  ctx.strokeStyle = '#f97316';
  ctx.lineWidth = 2;
  ctx.beginPath();
  const [sx, sy] = worldToCanvas(rectanglePoints[0]);
  ctx.moveTo(sx, sy);
  for (let i = 1; i < rectanglePoints.length; i += 1) {
    const [cx, cy] = worldToCanvas(rectanglePoints[i]);
    ctx.lineTo(cx, cy);
  }
  ctx.stroke();
  ctx.fillStyle = '#f97316';
  rectanglePoints.forEach((pt) => {
    const [cx, cy] = worldToCanvas(pt);
    ctx.beginPath();
    ctx.arc(cx, cy, 4, 0, Math.PI * 2);
    ctx.fill();
  });
}

function drawFrontPoints() {
  if (!frontPoints.length) return;
  ctx.strokeStyle = '#7c3aed';
  ctx.lineWidth = 2;
  ctx.beginPath();
  const [sx, sy] = worldToCanvas(frontPoints[0]);
  ctx.moveTo(sx, sy);
  for (let i = 1; i < frontPoints.length; i += 1) {
    const [cx, cy] = worldToCanvas(frontPoints[i]);
    ctx.lineTo(cx, cy);
  }
  ctx.stroke();
  ctx.fillStyle = '#7c3aed';
  frontPoints.forEach((pt) => {
    const [cx, cy] = worldToCanvas(pt);
    ctx.beginPath();
    ctx.arc(cx, cy, 4, 0, Math.PI * 2);
    ctx.fill();
  });
}

function drawFootprint() {
  if (!footprintWorld.length) return;
  ctx.strokeStyle = '#15803d';
  ctx.lineWidth = 2;
  ctx.beginPath();
  const [sx, sy] = worldToCanvas(footprintWorld[0]);
  ctx.moveTo(sx, sy);
  for (let i = 1; i < footprintWorld.length; i += 1) {
    const [cx, cy] = worldToCanvas(footprintWorld[i]);
    ctx.lineTo(cx, cy);
  }
  ctx.closePath();
  ctx.stroke();
}

function drawFrontVector() {
  if (!frontOrigin || !frontVector) return;
  ctx.strokeStyle = '#dc2626';
  ctx.lineWidth = 2;
  const start = worldToCanvas(frontOrigin);
  const length = Math.hypot(frontVector[0], frontVector[1]) || 1;
  const diag = Math.hypot(viewBox.maxX - viewBox.minX, viewBox.maxY - viewBox.minY) || 1;
  const scaleFactor = (diag * 0.3) / length;
  const endWorld = [
    frontOrigin[0] + frontVector[0] * scaleFactor,
    frontOrigin[1] + frontVector[1] * scaleFactor,
  ];
  const end = worldToCanvas(endWorld);
  ctx.beginPath();
  ctx.moveTo(start[0], start[1]);
  ctx.lineTo(end[0], end[1]);
  ctx.stroke();
  ctx.fillStyle = '#dc2626';
  ctx.beginPath();
  ctx.arc(start[0], start[1], 4, 0, Math.PI * 2);
  ctx.fill();
}

function drawCanvas() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  drawGeometry();
  drawRectanglePreview();
  drawFrontPoints();
  drawFootprint();
  drawFrontVector();
}

function updateSummaries() {
  footprintSummary.textContent = footprintWorld.length.toString();
  if (frontVector) {
    const norm = Math.hypot(frontVector[0], frontVector[1]) || 1;
    const normalized = frontVector.map((n) => Number((n / norm).toFixed(2)));
    frontSummary.textContent = `[${normalized.join(', ')}]`;
  } else {
    frontSummary.textContent = 'None';
  }
  updateStartButton();
}

canvas.addEventListener('click', (event) => {
  const rect = canvas.getBoundingClientRect();
  const scaleX = canvas.width / rect.width;
  const scaleY = canvas.height / rect.height;
  const rawX = (event.clientX - rect.left) * scaleX;
  const rawY = (event.clientY - rect.top) * scaleY;
  const worldPoint = canvasToWorld(rawX, rawY);
  if (captureMode === 'footprint') {
    if (rectanglePoints.length >= 3) rectanglePoints = [];
    rectanglePoints.push(worldPoint);
  } else {
    if (frontPoints.length >= 2) frontPoints = [];
    frontPoints.push(worldPoint);
  }
  drawCanvas();
});

footprintModeBtn.addEventListener('click', () => {
  captureMode = 'footprint';
  setStatus('Footprint mode active.');
});
frontModeBtn.addEventListener('click', () => {
  captureMode = 'front';
  setStatus('Front mode active.');
});
clearCanvasBtn.addEventListener('click', () => {
  rectanglePoints = [];
  frontPoints = [];
  frontOrigin = null;
  frontVector = null;
  footprintWorld = [];
  drawCanvas();
  updateSummaries();
});
applyCaptureBtn.addEventListener('click', async () => {
  if (!currentFilename) {
    alert('Select a DXF file first.');
    return;
  }
  if (rectanglePoints.length < 3) {
    alert('Select three points (A,B,C) for the rectangle.');
    return;
  }
  if (frontPoints.length < 2) {
    alert('Select two frontage points.');
    return;
  }
  try {
    const resp = await fetch(`/files/${encodeURIComponent(currentFilename)}/shrinkwrap`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rectangle_points: rectanglePoints,
        front_points: frontPoints,
      }),
    });
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    footprintWorld = data.footprint_points || [];
    frontOrigin = data.front_origin || null;
    frontVector = data.front_direction || null;
    shrinkwrapReady = true;
    rectanglePoints = [];
    frontPoints = [];
    recomputeViewBox();
    drawCanvas();
    updateSummaries();
    captureModal.classList.add('hidden');
    setStatus('Shrink-wrap captured. Ready to start crawl.');
    startButton.disabled = false;
    startButton.textContent = 'Start Crawl';
    saveDesignBtn.disabled = false;
  } catch (err) {
    setStatus(`Shrink-wrap failed: ${err}`);
  }
});

openCaptureBtn.addEventListener('click', async () => {
  const option = dxfSelect.options[dxfSelect.selectedIndex];
  if (!option || !option.dataset.filename) {
    alert('Select a DXF file first.');
    return;
  }
  currentFilename = option.dataset.filename;
  try {
    await loadGeometry(currentFilename);
    captureModal.classList.remove('hidden');
  } catch (err) {
    setStatus(`Preview failed: ${err}`);
  }
});
closeCaptureBtn.addEventListener('click', () => {
  captureModal.classList.add('hidden');
});
window.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') captureModal.classList.add('hidden');
});

async function loadGeometry(filename) {
  const resp = await fetch(`/files/${encodeURIComponent(filename)}/geometry`);
  if (!resp.ok) throw new Error(await resp.text());
  const data = await resp.json();
  geometryPaths = data.paths || [];
  rectanglePoints = [];
  frontPoints = [];
  footprintWorld = [];
  frontOrigin = null;
  frontVector = null;
  shrinkwrapReady = false;
  recomputeViewBox();
  drawCanvas();
  updateSummaries();
  updateStartButton();
}

async function handleJob(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const fileUrl = formData.get('dxf');
  if (!fileUrl) {
    alert('Select a remote DXF file.');
    return;
  }
  if (footprintWorld.length < 3 || !frontVector) {
    alert('Capture the footprint + front direction first.');
    return;
  }
  const payload = {
    address: formData.get('address'),
    dxf_url: fileUrl,
    config: {
      cycles: Number(formData.get('cycles') || 1),
      buffer: Number(formData.get('buffer') || 80),
      rotation_step: Number(formData.get('rotation') || 15),
      score_workers: Number(formData.get('score_workers') || 1),
    },
    footprint_points: footprintWorld.map(([x, y]) => [Number(x.toFixed(3)), Number(y.toFixed(3))]),
  };
  const norm = Math.hypot(frontVector[0], frontVector[1]) || 1;
  payload.front_direction = frontVector.map((n) => Number((n / norm).toFixed(4)));

  setStatus('Starting crawl…', true);
  try {
    const resp = await fetch('/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) throw new Error(await resp.text());
    const job = await resp.json();
    resultBox.textContent = JSON.stringify(job, null, 2);
    setStatus(`Job ${job.id} queued.`);
    updateHistory(job);
    renderJobDetails(job);
    pollJob(job.id);
  } catch (err) {
    setStatus(`Job request failed: ${err}`);
  }
}

async function pollJob(jobId) {
  if (pollTimer) clearTimeout(pollTimer);
  try {
    const resp = await fetch(`/jobs/${jobId}`);
    if (!resp.ok) throw new Error(await resp.text());
    const job = await resp.json();
    resultBox.textContent = JSON.stringify(job, null, 2);
    setStatus(`Job ${job.id}: ${job.status}`);
    updateHistory(job);
    renderJobDetails(job);
    fetchGeo(jobId);
    if (['queued', 'running'].includes(job.status)) {
      pollTimer = setTimeout(() => pollJob(jobId), 4000);
    }
  } catch (err) {
    statusText.textContent = `Polling failed: ${err}`;
  }
}

refreshBtn.addEventListener('click', refreshFiles);
uploadForm.addEventListener('submit', handleUpload);
jobForm.addEventListener('submit', handleJob);
footprintModeBtn.addEventListener('click', () => { captureMode = 'footprint'; });
frontModeBtn.addEventListener('click', () => { captureMode = 'front'; });

initPage();

function renderJobDetails(job) {
  if (!job) {
    jobDetails.innerHTML = '<p class="placeholder">No job data yet.</p>';
    logTailBox.textContent = '(log tail will appear here as jobs update)';
    renderArtifactsData(null);
    return;
  }

  const fragment = document.createDocumentFragment();
  const entries = [
    ['Job ID', job.id],
    ['Status', job.status],
    ['Address', job.address || '—'],
    ['DXF URL', job.dxf_url || '—'],
    ['Result URL', job.result_url || '—'],
    ['Error', job.error || '—'],
  ];
  let configText = null;

  if (job.config) {
    configText = JSON.stringify(job.config, null, 2);
  }

  const result = job.result || {};
  if (result.workspace) {
    entries.push(['Workspace', result.workspace]);
  }
  if (result.manifest_path) {
    entries.push(['Manifest', result.manifest_path]);
  }
  if (result.command) {
    entries.push(['Command', result.command]);
  }

  jobDetails.innerHTML = '';
  entries.forEach(([label, value]) => {
    const card = document.createElement('div');
    card.className = 'detail';
    const title = document.createElement('p');
    title.className = 'label';
    title.textContent = label;
    const body = document.createElement('p');
    body.className = 'value';
    if (label === 'Result URL' && value && value !== '—') {
      const link = document.createElement('a');
      link.href = value;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = 'Open';
      body.appendChild(link);
    } else if (label === 'DXF URL' && value && value !== '—') {
      body.textContent = value;
    } else {
      body.textContent = value || '—';
    }
    card.appendChild(title);
    card.appendChild(body);
    fragment.appendChild(card);
  });

  if (configText) {
    const card = document.createElement('div');
    card.className = 'detail detail-config';
    const title = document.createElement('p');
    title.className = 'label';
    title.textContent = 'Config';
    const body = document.createElement('div');
    body.className = 'value';
    const detailsEl = document.createElement('details');
    const summary = document.createElement('summary');
    summary.textContent = 'Show runtime config';
    const pre = document.createElement('pre');
    pre.textContent = configText;
    detailsEl.appendChild(summary);
    detailsEl.appendChild(pre);
    body.appendChild(detailsEl);
    card.appendChild(title);
    card.appendChild(body);
    fragment.appendChild(card);
  }

  if (!fragment.childNodes.length) {
    jobDetails.innerHTML = '<p class="placeholder">No job details available.</p>';
  } else {
    jobDetails.appendChild(fragment);
  }
  logTailBox.textContent = 'Loading logs…';
  fetchJobLog(job.id);
  if (result && result.artifacts) {
    renderArtifactsData(result.artifacts);
  } else {
    renderArtifactsData(null);
  }
  fetchJobArtifacts(job.id);
}

async function fetchJobLog(jobId) {
  if (!jobId) {
    logTailBox.textContent = '(log tail will appear here as jobs update)';
    return;
  }
  try {
    const resp = await fetch(`/jobs/${jobId}/logs?lines=400`);
    if (resp.status === 404) {
      logTailBox.textContent = 'Log is not available yet.';
      return;
    }
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    logTailBox.textContent = data.log_tail || '(no log output yet)';
  } catch (err) {
    logTailBox.textContent = `Failed to load logs: ${err}`;
  }
}

renderJobDetails(null);

function renderArtifactsData(artifacts) {
  if (!artifacts) {
    artifactsTableBody.innerHTML = '<tr><td colspan="2" class="placeholder">Artifacts will appear once a job finishes.</td></tr>';
    return;
  }

  const rows = [];

  const addRow = (label, value, link) => {
    if (value === undefined || value === null || value === '') return;
    rows.push([label, value, link]);
  };

  const walk = (prefix, value) => {
    if (Array.isArray(value)) {
      value.forEach((entry, index) => {
        const childPrefix = prefix ? `${prefix}[${index + 1}]` : `[${index + 1}]`;
        walk(childPrefix, entry);
      });
      return;
    }

    if (value && typeof value === 'object') {
      Object.keys(value).forEach((key) => {
        if (key.endsWith('_url')) return;
        const child = value[key];
        const childUrl = value[`${key}_url`];
        const label = prefix ? `${prefix}.${key}` : key;
        if (Array.isArray(child) || (child && typeof child === 'object')) {
          walk(label, child);
        } else {
          addRow(label, child, childUrl);
        }
      });
      return;
    }

    addRow(prefix, value, undefined);
  };

  walk('', artifacts);

  if (!rows.length) {
    artifactsTableBody.innerHTML = '<tr><td colspan="2" class="placeholder">Artifacts will appear once a job finishes.</td></tr>';
    return;
  }

  artifactsTableBody.innerHTML = '';
  rows.forEach(([label, value, link]) => {
    const tr = document.createElement('tr');
    const labelCell = document.createElement('td');
    labelCell.textContent = label;
    const valueCell = document.createElement('td');

    if (link) {
      const anchor = document.createElement('a');
      anchor.href = link;
      anchor.target = '_blank';
      anchor.rel = 'noopener noreferrer';
      anchor.textContent = value;
      valueCell.appendChild(anchor);
    } else if (typeof value === 'string' && value.startsWith('http')) {
      const anchor = document.createElement('a');
      anchor.href = value;
      anchor.target = '_blank';
      anchor.rel = 'noopener noreferrer';
      anchor.textContent = value;
      valueCell.appendChild(anchor);
    } else if (typeof value === 'string' && value.startsWith('/')) {
      const code = document.createElement('code');
      code.textContent = value;
      valueCell.appendChild(code);
    } else if (typeof value === 'string') {
      valueCell.textContent = value;
    } else {
      const pre = document.createElement('pre');
      pre.textContent = JSON.stringify(value, null, 2);
      valueCell.appendChild(pre);
    }

    tr.appendChild(labelCell);
    tr.appendChild(valueCell);
    artifactsTableBody.appendChild(tr);
  });
}

async function fetchJobArtifacts(jobId) {
  if (!jobId) return;
  try {
    const resp = await fetch(`/jobs/${jobId}/artifacts`);
    if (resp.status === 404) {
      if (!artifactsTableBody.innerHTML) {
        artifactsTableBody.innerHTML = '<tr><td colspan="2" class="placeholder">Artifacts will appear once a job finishes.</td></tr>';
      }
      return;
    }
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    renderArtifactsData(data.artifacts || null);
  } catch (err) {
    artifactsTableBody.innerHTML = `<tr><td colspan="2" class="placeholder">Failed to load artifacts: ${err}</td></tr>`;
  }
}

async function refreshDesigns() {
  try {
    const resp = await fetch('/designs');
    if (!resp.ok) throw new Error(await resp.text());
    const designs = await resp.json();
    designsList.innerHTML = '';
    if (!designs.length) {
      designsList.innerHTML = '<li class="placeholder">No designs saved yet.</li>';
      return;
    }
    designs.forEach((design) => {
      const li = document.createElement('li');
      li.textContent = `${design.name} (${design.slug})`;
      li.dataset.slug = design.slug;
      li.dataset.dxfUrl = design.dxf_url;
      li.dataset.footprint = JSON.stringify(design.footprint_points || []);
      li.dataset.front = JSON.stringify(design.front_direction || []);
      li.addEventListener('click', () => {
        const footprint = JSON.parse(li.dataset.footprint || '[]');
        const front = JSON.parse(li.dataset.front || '[]');
        drawDesignPreview(footprint, front);
        selectedDesign = design;
        footprintWorld = footprint;
        frontVector = front;
        shrinkwrapReady = true;
        updateSummaries();
        updateStartButton();
        setStatus(`Loaded design "${design.name}" into the form.`);
      });
      designsList.appendChild(li);
    });
  } catch (err) {
    designsList.innerHTML = `<li class="placeholder">Failed to load designs: ${err}</li>`;
  }
}

saveDesignBtn.addEventListener('click', async () => {
  if (!footprintWorld.length || !frontVector) {
    alert('Capture a footprint and front direction first.');
    return;
  }
  const name = designNameInput.value.trim();
  if (!name) {
    alert('Enter a design name.');
    return;
  }
  const option = dxfSelect.options[dxfSelect.selectedIndex];
  const dxfUrl = option ? option.value : '';
  if (!dxfUrl) {
    alert('Select a DXF before saving a design.');
    return;
  }
  const payload = {
    name,
    dxf_url: dxfUrl,
    footprint_points: footprintWorld,
    front_direction: frontVector,
  };
  try {
    const resp = await fetch('/designs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) throw new Error(await resp.text());
    setStatus(`Saved design "${name}".`);
    designNameInput.value = '';
    await refreshDesigns();
  } catch (err) {
    setStatus(`Save design failed: ${err}`);
  }
});

function initTabs() {
  // No-op for multi-page layout
}

function initMapTab() {
  // Populate map design selector from saved designs list
  const syncDesignSelect = () => {
    if (!mapDesignSelect || !designsList) return;
    mapDesignSelect.innerHTML = '<option value="">-- Select a saved design --</option>';
    designsList.querySelectorAll('li').forEach((li) => {
      const slug = li.dataset.slug;
      if (!slug) return;
      const name = li.textContent || slug;
      const option = document.createElement('option');
      option.value = slug;
      option.textContent = name;
      mapDesignSelect.appendChild(option);
    });
  };
  if (designsList) {
    const observer = new MutationObserver(syncDesignSelect);
    observer.observe(designsList, { childList: true });
  }
  syncDesignSelect();

  // Leaflet
  if (!document.getElementById('map')) return;
  const script = document.createElement('script');
  script.src = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js';
  script.integrity = 'sha256-pMh7CkP0iE1JfTIyekN3rN6QYP8K9PsM0qzA+3F3mXY=';
  script.crossOrigin = '';
  script.onload = () => {
    initLeafletMap(syncDesignSelect);
  };
  document.head.appendChild(script);
  const css = document.createElement('link');
  css.rel = 'stylesheet';
  css.href = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
  document.head.appendChild(css);
}

function initLeafletMap(onDesignsReady) {
  const mapEl = document.getElementById('map');
  if (!mapEl) return;
  mapInstance = L.map(mapEl).setView([37.8, -96], 4);
  const maptilerKey = window.MAPTILER_KEY || '';
  const tileUrl = maptilerKey
    ? `https://api.maptiler.com/maps/streets-v2/{z}/{x}/{y}.png?key=${maptilerKey}`
    : 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  const attribution = maptilerKey
    ? '&copy; MapTiler & OpenStreetMap contributors'
    : '&copy; OpenStreetMap contributors';
  L.tileLayer(tileUrl, { maxZoom: 19, attribution }).addTo(mapInstance);
  geoLayer = L.geoJSON().addTo(mapInstance);
  let marker = null;

  const startMapJob = async (lat, lng) => {
    const selectedSlug = mapDesignSelect.value;
    if (!selectedSlug) {
      mapStatus.textContent = 'Select a saved design first.';
      return;
    }
    const li = Array.from(designsList.querySelectorAll('li')).find((item) => item.dataset.slug === selectedSlug);
    if (!li) {
      mapStatus.textContent = 'Design not found.';
      return;
    }
    const footprint = JSON.parse(li.dataset.footprint || '[]');
    const front = JSON.parse(li.dataset.front || '[]');
    const dxfUrl = li.dataset.dxfUrl || '';
    if (!dxfUrl) {
      mapStatus.textContent = 'Design missing DXF URL.';
      return;
    }
    const address = mapAddressInput.value.trim() || `Pin at ${lat.toFixed(5)},${lng.toFixed(5)}`;
    const payload = {
      address,
      dxf_url: dxfUrl,
      config: {
        cycles: Number(cfgCycles.value || 1),
        buffer: Number(cfgBuffer.value || 80),
        rotation_step: Number(cfgRotation.value || 15),
        score_workers: Number(cfgScoreWorkers.value || 1),
      },
      footprint_points: footprint,
      front_direction: front,
    };
    try {
      mapStatus.textContent = 'Starting crawl from map pin…';
      const resp = await fetch('/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!resp.ok) throw new Error(await resp.text());
      const job = await resp.json();
      setStatus(`Job ${job.id} queued from map pin.`);
      resultBox.textContent = JSON.stringify(job, null, 2);
      updateHistory(job);
      renderJobDetails(job);
      pollJob(job.id);
      mapStatus.textContent = `Job ${job.id} queued (pin ${lat.toFixed(5)},${lng.toFixed(5)}).`;
      // kick off geo polling for overlays
      fetchGeo(job.id);
    } catch (err) {
      mapStatus.textContent = `Failed to start crawl: ${err}`;
    } finally {
      mapProgress.classList.add('hidden');
    }
  };

  mapInstance.on('click', (evt) => {
    const { lat, lng } = evt.latlng;
    if (marker) marker.remove();
    marker = L.marker([lat, lng]).addTo(mapInstance);
    mapStatus.textContent = `Pin at ${lat.toFixed(5)}, ${lng.toFixed(5)}. Starting crawl…`;
    startMapJob(lat, lng);
  });

  if (onDesignsReady) onDesignsReady();
}

function drawDesignPreview(points = [], front = null) {
  if (!designPreview) return;
  const context = designPreview.getContext('2d');
  context.clearRect(0, 0, designPreview.width, designPreview.height);
  if (!points.length) return;
  const xs = points.map((p) => p[0]);
  const ys = points.map((p) => p[1]);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const paddingPx = 10;
  const scale = Math.min(
    (designPreview.width - 2 * paddingPx) / Math.max(1e-6, maxX - minX),
    (designPreview.height - 2 * paddingPx) / Math.max(1e-6, maxY - minY),
  );
  const toCanvas = ([x, y]) => {
    const cx = (x - minX) * scale + paddingPx;
    const cy = designPreview.height - ((y - minY) * scale + paddingPx);
    return [cx, cy];
  };

  context.strokeStyle = '#93c5fd';
  context.lineWidth = 2;
  context.beginPath();
  points.forEach((pt, idx) => {
    const [cx, cy] = toCanvas(pt);
    if (idx === 0) context.moveTo(cx, cy);
    else context.lineTo(cx, cy);
  });
  context.closePath();
  context.stroke();

  if (front) {
    const origin = toCanvas(points[0]);
    const vec = [origin[0] + front[0] * 20, origin[1] - front[1] * 20];
    context.strokeStyle = '#22c55e';
    context.beginPath();
    context.moveTo(origin[0], origin[1]);
    context.lineTo(vec[0], vec[1]);
    context.stroke();
  }
}

async function fetchGeo(jobId) {
  if (!geoLayer || !jobId) return;
  try {
    const resp = await fetch(`/jobs/${jobId}/geo`);
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    geoLayer.clearLayers();
    geoLayer.addData(data.features || []);
    mapStatus.textContent = `Parcels completed: ${data.progress.completed}/${data.progress.total}`;
  } catch (err) {
    // fail silently to avoid UI noise
  }
}
