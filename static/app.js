const filesTableBody = document.querySelector('#filesTable tbody');
const refreshBtn = document.getElementById('refreshFiles');
const uploadForm = document.getElementById('uploadForm');
const uploadFileInput = document.getElementById('uploadFile');
const jobForm = document.getElementById('jobForm');
const startButton = document.getElementById('startButton');
const dxfSelect = document.getElementById('dxfSelect');
const statusText = document.getElementById('statusText');
const resultBox = document.getElementById('resultBox');
const historyTableBody = document.querySelector('#historyTable tbody');

const captureModal = document.getElementById('captureModal');
const openCaptureBtn = document.getElementById('openCapture');
const closeCaptureBtn = document.getElementById('closeCapture');
const applyCaptureBtn = document.getElementById('applyCapture');
const footprintSummary = document.getElementById('footprintSummary');
const frontSummary = document.getElementById('frontSummary');

const canvas = document.getElementById('captureCanvas');
const ctx = canvas.getContext('2d');
const footprintModeBtn = document.getElementById('footprintMode');
const frontModeBtn = document.getElementById('frontMode');
const clearCanvasBtn = document.getElementById('clearCanvas');

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

refreshFiles();
recomputeViewBox();
drawCanvas();
updateSummaries();
updateStartButton();
