const filesTableBody = document.querySelector('#filesTable tbody');
const refreshBtn = document.getElementById('refreshFiles');
const uploadForm = document.getElementById('uploadForm');
const uploadFileInput = document.getElementById('uploadFile');
const jobForm = document.getElementById('jobForm');
const dxfSelect = document.getElementById('dxfSelect');
const statusText = document.getElementById('statusText');
const resultBox = document.getElementById('resultBox');

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
let remoteFiles = [];
let footprintWorld = [];
let frontOrigin = null;
let frontVector = null;
let captureMode = 'footprint';
let frontStage = 0;
let viewBox = { minX: 0, maxX: 100, minY: 0, maxY: 100, scale: 1 };

async function refreshFiles() {
  try {
    const resp = await fetch('/files');
    if (!resp.ok) throw new Error('Request failed');
    remoteFiles = await resp.json();
    filesTableBody.innerHTML = '';
    dxfSelect.innerHTML = '<option value="">-- Select a file --</option>';
    remoteFiles.forEach((file) => {
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
      delBtn.addEventListener('click', () => confirmDelete(file.filename));
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
    statusText.textContent = `Loaded ${remoteFiles.length} files.`;
  } catch (err) {
    statusText.textContent = `Failed to fetch files: ${err}`;
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
  statusText.textContent = 'Uploading…';
  try {
    const resp = await fetch('/files', { method: 'POST', body: formData });
    if (!resp.ok) throw new Error(await resp.text());
    const payload = await resp.json();
    statusText.textContent = `Uploaded ${payload.filename}`;
    resultBox.textContent = JSON.stringify(payload, null, 2);
    await refreshFiles();
    Array.from(dxfSelect.options).forEach((opt) => {
      if (opt.dataset.filename === payload.filename) {
        opt.selected = true;
      }
    });
  } catch (err) {
    statusText.textContent = `Upload failed: ${err}`;
  }
}

function computeViewBox() {
  const xs = footprintWorld.map((p) => p[0]);
  const ys = footprintWorld.map((p) => p[1]);
  if (frontOrigin) {
    xs.push(frontOrigin[0]);
    ys.push(frontOrigin[1]);
  }
  if (frontOrigin && frontVector) {
    xs.push(frontOrigin[0] + frontVector[0]);
    ys.push(frontOrigin[1] + frontVector[1]);
  }
  if (!xs.length) {
    viewBox = { minX: 0, maxX: 100, minY: 0, maxY: 100, scale: 1 };
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

function drawCanvas() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.strokeStyle = '#0f172a';
  ctx.lineWidth = 2;
  if (footprintWorld.length) {
    ctx.beginPath();
    const [sx, sy] = worldToCanvas(footprintWorld[0]);
    ctx.moveTo(sx, sy);
    for (let i = 1; i < footprintWorld.length; i += 1) {
      const [cx, cy] = worldToCanvas(footprintWorld[i]);
      ctx.lineTo(cx, cy);
    }
    ctx.stroke();
    ctx.closePath();
  }
  ctx.fillStyle = '#2563eb';
  footprintWorld.forEach((point) => {
    const [cx, cy] = worldToCanvas(point);
    ctx.beginPath();
    ctx.arc(cx, cy, 4, 0, Math.PI * 2);
    ctx.fill();
  });

  if (frontOrigin && frontVector) {
    const start = worldToCanvas(frontOrigin);
    const length = Math.hypot(frontVector[0], frontVector[1]) || 1;
    const diag = Math.hypot(viewBox.maxX - viewBox.minX, viewBox.maxY - viewBox.minY) || 1;
    const scaleFactor = (diag * 0.3) / length;
    const endWorld = [
      frontOrigin[0] + frontVector[0] * scaleFactor,
      frontOrigin[1] + frontVector[1] * scaleFactor,
    ];
    const end = worldToCanvas(endWorld);
    ctx.strokeStyle = '#dc2626';
    ctx.beginPath();
    ctx.moveTo(start[0], start[1]);
    ctx.lineTo(end[0], end[1]);
    ctx.stroke();
    ctx.fillStyle = '#dc2626';
    ctx.beginPath();
    ctx.arc(start[0], start[1], 4, 0, Math.PI * 2);
    ctx.fill();
  } else if (frontOrigin) {
    const [cx, cy] = worldToCanvas(frontOrigin);
    ctx.fillStyle = '#dc2626';
    ctx.beginPath();
    ctx.arc(cx, cy, 4, 0, Math.PI * 2);
    ctx.fill();
  }
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
}

canvas.addEventListener('click', (event) => {
  const rect = canvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const worldPoint = canvasToWorld(x, y);
  if (captureMode === 'footprint') {
    footprintWorld.push(worldPoint);
  } else {
    if (frontStage === 0) {
      frontOrigin = worldPoint;
      frontVector = null;
      frontStage = 1;
    } else {
      frontVector = [worldPoint[0] - frontOrigin[0], worldPoint[1] - frontOrigin[1]];
      frontStage = 0;
    }
  }
  computeViewBox();
  updateSummaries();
  drawCanvas();
});

footprintModeBtn.addEventListener('click', () => {
  captureMode = 'footprint';
  statusText.textContent = 'Footprint mode active.';
});
frontModeBtn.addEventListener('click', () => {
  captureMode = 'front';
  statusText.textContent = 'Front mode active.';
});
clearCanvasBtn.addEventListener('click', () => {
  footprintWorld = [];
  frontOrigin = null;
  frontVector = null;
  frontStage = 0;
  computeViewBox();
  drawCanvas();
  updateSummaries();
});
applyCaptureBtn.addEventListener('click', () => {
  if (footprintWorld.length < 3) {
    alert('Add at least three footprint points.');
    return;
  }
  if (!frontVector) {
    alert('Add two frontage points.');
    return;
  }
  statusText.textContent = 'Footprint/front captured.';
  captureModal.classList.add('hidden');
});
openCaptureBtn.addEventListener('click', async () => {
  const option = dxfSelect.options[dxfSelect.selectedIndex];
  if (!option || !option.dataset.filename) {
    alert('Select a DXF file first.');
    return;
  }
  try {
    await loadPreview(option.dataset.filename);
    captureModal.classList.remove('hidden');
  } catch (err) {
    statusText.textContent = `Preview failed: ${err}`;
  }
});
closeCaptureBtn.addEventListener('click', () => {
  captureModal.classList.add('hidden');
});
window.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    captureModal.classList.add('hidden');
  }
});

async function loadPreview(filename) {
  const resp = await fetch(`/files/${encodeURIComponent(filename)}/preview`);
  if (!resp.ok) throw new Error(await resp.text());
  const data = await resp.json();
  footprintWorld = data.footprint_points || [];
  frontOrigin = data.front_origin || null;
  frontVector = data.front_direction || null;
  frontStage = 0;
  computeViewBox();
  drawCanvas();
  updateSummaries();
}

async function handleJob(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const fileUrl = formData.get('dxf');
  if (!fileUrl) {
    alert('Select a remote DXF file.');
    return;
  }
  if (footprintWorld.length < 3) {
    alert('Capture the footprint points.');
    return;
  }
  if (!frontVector) {
    alert('Capture the front direction.');
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
  const length = Math.hypot(frontVector[0], frontVector[1]) || 1;
  payload.front_direction = frontVector.map((n) => Number((n / length).toFixed(4)));

  statusText.textContent = 'Starting crawl…';
  try {
    const resp = await fetch('/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) throw new Error(await resp.text());
    const job = await resp.json();
    statusText.textContent = `Job ${job.id} queued.`;
    resultBox.textContent = JSON.stringify(job, null, 2);
    pollJob(job.id);
  } catch (err) {
    statusText.textContent = `Job request failed: ${err}`;
  }
}

async function confirmDelete(filename) {
  if (!window.confirm(`Delete ${filename}?`)) return;
  statusText.textContent = `Deleting ${filename}…`;
  try {
    const resp = await fetch(`/files/${encodeURIComponent(filename)}`, {
      method: 'DELETE',
    });
    if (!resp.ok) throw new Error(await resp.text());
    await refreshFiles();
    statusText.textContent = `Deleted ${filename}.`;
  } catch (err) {
    statusText.textContent = `Delete failed: ${err}`;
  }
}

async function pollJob(jobId) {
  if (pollTimer) clearTimeout(pollTimer);
  try {
    const resp = await fetch(`/jobs/${jobId}`);
    if (!resp.ok) throw new Error(await resp.text());
    const job = await resp.json();
    resultBox.textContent = JSON.stringify(job, null, 2);
    statusText.textContent = `Job ${job.id}: ${job.status}`;
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
footprintModeBtn.addEventListener('click', () => {
  captureMode = 'footprint';
});
frontModeBtn.addEventListener('click', () => {
  captureMode = 'front';
});

refreshFiles();
computeViewBox();
drawCanvas();
updateSummaries();
