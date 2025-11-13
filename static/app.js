const filesTableBody = document.querySelector('#filesTable tbody');
const refreshBtn = document.getElementById('refreshFiles');
const uploadForm = document.getElementById('uploadForm');
const uploadFileInput = document.getElementById('uploadFile');
const jobForm = document.getElementById('jobForm');
const dxfSelect = document.getElementById('dxfSelect');
const statusText = document.getElementById('statusText');
const resultBox = document.getElementById('resultBox');

const captureModal = document.getElementById('captureModal');
const canvas = document.getElementById('captureCanvas');
const ctx = canvas.getContext('2d');
const footprintSummary = document.getElementById('footprintSummary');
const frontSummary = document.getElementById('frontSummary');
const footprintModeBtn = document.getElementById('footprintMode');
const frontModeBtn = document.getElementById('frontMode');
const clearCanvasBtn = document.getElementById('clearCanvas');
const applyCaptureBtn = document.getElementById('applyCapture');
const openCaptureBtn = document.getElementById('openCapture');
const closeCaptureBtn = document.getElementById('closeCapture');

let pollTimer = null;
let footprintPoints = [];
let frontPoints = [];
let captureMode = 'footprint'; // or 'front'

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
      row.appendChild(nameCell);
      row.appendChild(linkCell);
      filesTableBody.appendChild(row);

      const opt = document.createElement('option');
      opt.value = file.file_url;
      opt.textContent = file.filename;
      dxfSelect.appendChild(opt);
    });
    statusText.textContent = `Loaded ${data.length} files.`;
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
    refreshFiles();
  } catch (err) {
    statusText.textContent = `Upload failed: ${err}`;
  }
}

async function handleJob(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const fileUrl = formData.get('dxf');
  if (!fileUrl) {
    alert('Select a remote DXF file.');
    return;
  }
  if (footprintPoints.length < 3) {
    alert('Capture at least three footprint points.');
    return;
  }
  if (frontPoints.length !== 2) {
    alert('Capture two points for the front direction.');
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
    footprint_points: footprintPoints.map(([x, y]) => [Number(x.toFixed(2)), Number(y.toFixed(2))]),
  };

  const frontVector = [
    frontPoints[1][0] - frontPoints[0][0],
    frontPoints[1][1] - frontPoints[0][1],
  ];
  payload.front_direction = frontVector.map((n) => Number(n.toFixed(2)));

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

function drawCanvas() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.strokeStyle = '#0f172a';
  ctx.lineWidth = 2;
  if (footprintPoints.length) {
    ctx.beginPath();
    ctx.moveTo(footprintPoints[0][0], footprintPoints[0][1]);
    for (let i = 1; i < footprintPoints.length; i += 1) {
      ctx.lineTo(footprintPoints[i][0], footprintPoints[i][1]);
    }
    ctx.stroke();
    ctx.closePath();
  }

  ctx.fillStyle = '#2563eb';
  footprintPoints.forEach(([x, y]) => {
    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fill();
  });

  if (frontPoints.length === 2) {
    ctx.strokeStyle = '#dc2626';
    ctx.beginPath();
    ctx.moveTo(frontPoints[0][0], frontPoints[0][1]);
    ctx.lineTo(frontPoints[1][0], frontPoints[1][1]);
    ctx.stroke();
  }
  ctx.fillStyle = '#dc2626';
  frontPoints.forEach(([x, y]) => {
    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fill();
  });
}

function openModal() {
  captureModal.classList.remove('hidden');
}

function closeModal() {
  captureModal.classList.add('hidden');
}

function updateSummaries() {
  footprintSummary.textContent = footprintPoints.length.toString();
  if (frontPoints.length === 2) {
    const vector = [
      (frontPoints[1][0] - frontPoints[0][0]).toFixed(1),
      (frontPoints[1][1] - frontPoints[0][1]).toFixed(1),
    ];
    frontSummary.textContent = `[${vector.join(', ')}]`;
  } else {
    frontSummary.textContent = 'None';
  }
}

canvas.addEventListener('click', (event) => {
  const rect = canvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  if (captureMode === 'footprint') {
    footprintPoints.push([x, y]);
  } else if (captureMode === 'front') {
    if (frontPoints.length === 2) frontPoints = [];
    frontPoints.push([x, y]);
  }
  updateSummaries();
  drawCanvas();
});

footprintModeBtn.addEventListener('click', () => {
  captureMode = 'footprint';
  statusText.textContent = 'Footprint mode active.';
});
frontModeBtn.addEventListener('click', () => {
  captureMode = 'front';
  statusText.textContent = 'Front vector mode active.';
});
clearCanvasBtn.addEventListener('click', () => {
  footprintPoints = [];
  frontPoints = [];
  drawCanvas();
  updateSummaries();
});
applyCaptureBtn.addEventListener('click', () => {
  if (footprintPoints.length < 3) {
    alert('Add at least three footprint points.');
    return;
  }
  if (frontPoints.length !== 2) {
    alert('Add two frontage points.');
    return;
  }
  statusText.textContent = 'Footprint/front captured.';
  closeModal();
});
openCaptureBtn.addEventListener('click', () => {
  openModal();
});
closeCaptureBtn.addEventListener('click', () => {
  closeModal();
});

window.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    closeModal();
  }
});

refreshBtn.addEventListener('click', refreshFiles);
uploadForm.addEventListener('submit', handleUpload);
jobForm.addEventListener('submit', handleJob);

refreshFiles();
drawCanvas();
updateSummaries();
