const filesTableBody = document.querySelector('#filesTable tbody');
const refreshBtn = document.getElementById('refreshFiles');
const uploadForm = document.getElementById('uploadForm');
const uploadFileInput = document.getElementById('uploadFile');
const jobForm = document.getElementById('jobForm');
const dxfSelect = document.getElementById('dxfSelect');
const statusText = document.getElementById('statusText');
const resultBox = document.getElementById('resultBox');

let pollTimer = null;

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
  const payload = {
    address: formData.get('address'),
    dxf_url: fileUrl,
    config: {
      cycles: Number(formData.get('cycles') || 1),
      buffer: Number(formData.get('buffer') || 80),
      rotation_step: Number(formData.get('rotation') || 15),
      score_workers: Number(formData.get('score_workers') || 1),
    },
  };

  const footprintText = formData.get('footprint');
  if (footprintText) {
    try {
      payload.footprint_points = JSON.parse(footprintText);
    } catch (err) {
      alert('Footprint JSON invalid.');
      return;
    }
  }

  const frontText = (formData.get('front') || '').trim();
  if (frontText) {
    const parts = frontText.split(',').map((n) => Number(n.trim()));
    if (parts.length !== 2 || parts.some((n) => Number.isNaN(n))) {
      alert('Front direction must be two comma-separated numbers.');
      return;
    }
    payload.front_direction = parts;
  }

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

refreshBtn.addEventListener('click', refreshFiles);
uploadForm.addEventListener('submit', handleUpload);
jobForm.addEventListener('submit', handleJob);

refreshFiles();
