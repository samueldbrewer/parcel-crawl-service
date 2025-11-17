document.addEventListener('DOMContentLoaded', () => {
  const mapEl = document.getElementById('map');
  const statusEl = document.getElementById('mapStatus');
  if (!mapEl || typeof L === 'undefined') return;

  const maptilerKey = window.MAPTILER_KEY || '';
  const tileUrl = maptilerKey
    ? `https://api.maptiler.com/maps/streets-v2/{z}/{x}/{y}.png?key=${maptilerKey}`
    : 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  const attribution = maptilerKey
    ? '&copy; MapTiler & OpenStreetMap contributors'
    : '&copy; OpenStreetMap contributors';

  const map = L.map(mapEl).setView([37.8, -96], 4);
  L.tileLayer(tileUrl, { maxZoom: 19, attribution }).addTo(map);

  if (statusEl) statusEl.textContent = maptilerKey ? 'Using MapTiler tiles.' : 'Using OSM tiles.';
});
