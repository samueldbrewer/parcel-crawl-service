document.addEventListener('DOMContentLoaded', () => {
  const mapEl = document.getElementById('map');
  const statusEl = document.getElementById('mapStatus');
  const logEl = document.getElementById('mapDebug');

  const log = (message, extra) => {
    console.log('[Map]', message, extra ?? '');
    if (!logEl) return;
    const line = document.createElement('div');
    line.className = 'log-entry';
    const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0];
    const details = extra ? `${message} ${JSON.stringify(extra)}` : message;
    line.textContent = `[${timestamp}] ${details}`;
    logEl.appendChild(line);
    logEl.scrollTop = logEl.scrollHeight;
  };

  const setStatus = (message) => {
    if (statusEl) statusEl.textContent = message;
    log(message);
  };

  if (!mapEl) {
    log('Map container not found on page.');
    return;
  }

  if (typeof L === 'undefined') {
    setStatus('Leaflet failed to load. Check network/CSP.');
    log('Leaflet global `L` is undefined. CDN may be blocked.');
    return;
  }

  const maptilerKey = (window.MAPTILER_KEY || '').trim();
  const tileProvider = maptilerKey ? 'MapTiler' : 'OpenStreetMap';
  const tileUrl = maptilerKey
    ? `https://api.maptiler.com/maps/streets-v2/{z}/{x}/{y}.png?key=${maptilerKey}`
    : 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  const attribution = maptilerKey
    ? '&copy; MapTiler & OpenStreetMap contributors'
    : '&copy; OpenStreetMap contributors';

  setStatus(`Initializing map with ${tileProvider} tiles...`);
  log('Bootstrap details', { maptilerKeyPresent: Boolean(maptilerKey), tileUrl });

  let map;
  try {
    map = L.map(mapEl, { preferCanvas: true }).setView([37.8, -96], 4);
  } catch (err) {
    setStatus('Could not create map. See console for details.');
    console.error('Failed to create Leaflet map instance', err);
    return;
  }

  const tiles = L.tileLayer(tileUrl, { maxZoom: 19, attribution });

  tiles.on('load', () => {
    setStatus(`${tileProvider} tiles loaded.`);
  });

  tiles.on('tileerror', (event) => {
    setStatus('Tile load failed; see debug log.');
    log('Tile load error', {
      coords: event.coords,
      message: event?.error?.message,
      url: event?.error?.target?.src || event?.tile?.src,
    });
  });

  tiles.addTo(map);

  map.on('load', () => log('Map render complete'));
  map.on('moveend', () => {
    const center = map.getCenter();
    log('Map moved', { lat: center.lat.toFixed(4), lng: center.lng.toFixed(4), zoom: map.getZoom() });
  });

  setStatus(`Using ${tileProvider} tiles.`);
});
