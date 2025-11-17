// Standalone map viewer script for the landing page.
document.addEventListener('DOMContentLoaded', () => {
  window.__MAP_VIEWER_JS_LOADED = true;

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

  log('Map viewer script executed; initializing...');
  if (typeof window.mapBootstrapLog === 'function') {
    window.mapBootstrapLog('map-viewer.js executed successfully.');
  }

  if (!mapEl) {
    log('Map container not found on page.');
    return;
  }

  if (typeof L === 'undefined') {
    setStatus('Leaflet failed to load. Check network/CSP.');
    log('Leaflet global `L` is undefined. CDN may be blocked.');
    return;
  }

  setStatus('Initializing map with OpenStreetMap tiles...');
  log('Bootstrap details', { tileProvider: 'OpenStreetMap' });

  // Paint a repeating tile as a visual fallback before Leaflet renders.
  const fallbackTileUrl = 'https://tile.openstreetmap.org/0/0/0.png';
  const fallbackTile = new Image();
  fallbackTile.onload = () => {
    mapEl.style.backgroundImage = `url(${fallbackTileUrl})`;
    mapEl.style.backgroundSize = '256px 256px';
    mapEl.style.backgroundRepeat = 'repeat';
    mapEl.style.backgroundPosition = 'center';
    log('Applied fallback OSM tile background.');
  };
  fallbackTile.onerror = () => log('Failed to load fallback OSM tile background.');
  fallbackTile.src = fallbackTileUrl;

  let map;
  try {
    map = L.map(mapEl, { preferCanvas: true }).setView([37.8, -96], 4);
  } catch (err) {
    setStatus('Could not create map. See console for details.');
    console.error('Failed to create Leaflet map instance', err);
    return;
  }

  const tiles = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
  });

  tiles.on('load', () => {
    setStatus('OpenStreetMap tiles loaded.');
    // Clear fallback background once real tiles are in.
    mapEl.style.backgroundImage = '';
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

  setStatus('Using OpenStreetMap tiles.');
});
