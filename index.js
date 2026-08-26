'use strict';

const http = require('node:http');
const path = require('node:path');
const { spawn } = require('node:child_process');

const PLUGIN_ID = 'signalk-charts-provider-progressive';
const BACKEND_HOST = '127.0.0.1';
const DEFAULT_BACKEND_PORT = 8787;
const DEFAULT_REFRESH_SECONDS = 3;
const MAX_CHARTS = 2_000;
const MAX_DESCRIPTOR_BYTES = 2 * 1024 * 1024;
const MAX_TILE_BYTES = 16 * 1024 * 1024;
const MAX_UI_BYTES = 2 * 1024 * 1024;
const MAX_API_RESPONSE_BYTES = 16 * 1024 * 1024;
const MAX_POST_BODY_BYTES = 512 * 1024;
const MAX_ZOOM = 22;
const TILE_PATH = '/signalk/v1/api/resources/charts';
const DESCRIPTOR_PATHS = ['/api/provider/charts', '/api/charts'];

const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const GENERATION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const LAYER_PATTERN = /^[A-Za-z0-9_:-]{1,80}$/;
const JOB_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

function pluginConstructor(app) {
  let config = normalizeConfig({});
  let charts = new Map();
  let child = null;
  let refreshTimer = null;
  let refreshInFlight = null;
  let lifecycle = 0;
  let providerRegistered = false;
  let lastBackendError = '';

  function debug(message) {
    if (typeof app.debug === 'function') {
      app.debug(`[progressive-charts] ${message}`);
    }
  }

  function reportError(message) {
    if (message === lastBackendError) {
      return;
    }
    lastBackendError = message;
    if (typeof app.error === 'function') {
      app.error(`[progressive-charts] ${message}`);
    }
  }

  function setStatus(message) {
    if (typeof app.setPluginStatus === 'function') {
      app.setPluginStatus(message);
    }
  }

  function currentV1() {
    return Object.fromEntries(
      [...charts.entries()].map(([identifier, chart]) => [identifier, toV1Descriptor(chart)])
    );
  }

  function currentV2() {
    return Object.fromEntries(
      [...charts.entries()].map(([identifier, chart]) => [identifier, toV2Descriptor(chart)])
    );
  }

  function emitChartDelta(identifier, value) {
    if (typeof app.handleMessage !== 'function') {
      return;
    }
    try {
      app.handleMessage(
        PLUGIN_ID,
        {
          updates: [
            {
              values: [
                {
                  path: `resources.charts.${identifier}`,
                  value
                }
              ]
            }
          ]
        },
        2
      );
    } catch (error) {
      reportError(`Unable to emit chart update for ${identifier}: ${errorMessage(error)}`);
    }
  }

  function replaceCharts(nextCharts, emitDeltas) {
    const previous = charts;
    const next = new Map(nextCharts.map((chart) => [chart.identifier, chart]));
    charts = next;

    if (!emitDeltas) {
      return;
    }

    for (const [identifier, chart] of next) {
      const old = previous.get(identifier);
      if (!old || descriptorFingerprint(old) !== descriptorFingerprint(chart)) {
        emitChartDelta(identifier, toV2Descriptor(chart));
      }
    }
    for (const identifier of previous.keys()) {
      if (!next.has(identifier)) {
        emitChartDelta(identifier, null);
      }
    }
  }

  async function refreshCharts(emitDeltas = true) {
    if (refreshInFlight) {
      return refreshInFlight;
    }
    const refreshLifecycle = lifecycle;
    refreshInFlight = (async () => {
      const next = await fetchChartSnapshot(config.backendPort);
      if (refreshLifecycle !== lifecycle) {
        return next;
      }
      replaceCharts(next, emitDeltas);
      lastBackendError = '';
      setStatus(
        `${next.length} progressive chart${next.length === 1 ? '' : 's'} available from the local chart worker`
      );
      return next;
    })();
    try {
      return await refreshInFlight;
    } finally {
      refreshInFlight = null;
    }
  }

  async function initialize(token) {
    let reachable = await backendIsReachable(config.backendPort);
    if (!reachable && config.startLocalBackend && token === lifecycle) {
      child = spawnLocalBackend(app, config.backendPort, debug, reportError);
      reachable = await waitForBackend(config.backendPort, 15_000, () => token !== lifecycle);
    }

    if (token !== lifecycle) {
      return;
    }

    if (!reachable) {
      const message = `The local chart worker is unavailable on ${BACKEND_HOST}:${config.backendPort}`;
      reportError(message);
      if (typeof app.setPluginError === 'function') {
        app.setPluginError(message);
      }
      setStatus('Waiting for the local chart worker');
    } else {
      try {
        await refreshCharts(true);
      } catch (error) {
        reportError(`Unable to refresh chart descriptors: ${errorMessage(error)}`);
        setStatus('The local chart worker is running, but chart descriptors are unavailable');
      }
    }

    if (token !== lifecycle) {
      return;
    }
    refreshTimer = setInterval(() => {
      void refreshCharts(true).catch((error) => {
        reportError(`Unable to refresh chart descriptors: ${errorMessage(error)}`);
        setStatus('Waiting for chart descriptor refresh');
      });
    }, config.refreshIntervalSeconds * 1_000);
    refreshTimer.unref?.();
  }

  function registerProvider() {
    if (providerRegistered || typeof app.registerResourceProvider !== 'function') {
      return;
    }
    app.registerResourceProvider({
      type: 'charts',
      methods: {
        listResources: () => Promise.resolve(currentV2()),
        getResource: (identifier) => {
          const chart = charts.get(identifier);
          if (!chart) {
            return Promise.reject(new Error('Chart not found'));
          }
          return Promise.resolve(toV2Descriptor(chart));
        },
        setResource: () => Promise.reject(new Error('Chart resources are read-only')),
        deleteResource: () => Promise.reject(new Error('Chart resources are read-only'))
      }
    });
    providerRegistered = true;
  }

  function stopRuntime() {
    lifecycle += 1;
    if (refreshTimer) {
      clearInterval(refreshTimer);
      refreshTimer = null;
    }
    if (child) {
      stopChild(child);
      child = null;
    }
    refreshInFlight = null;
  }

  const plugin = {
    id: PLUGIN_ID,
    name: 'Charts Provider Progressive',
    description:
      'Publishes locally generated progressive NOAA vector charts through the Signal K charts API.',
    schema: () => ({
      title: 'Charts Provider Progressive',
      type: 'object',
      additionalProperties: false,
      properties: {
        startLocalBackend: {
          type: 'boolean',
          title: 'Start the local chart worker',
          description:
            'Start the bundled chart worker on this Signal K host when it is not already running.',
          default: true
        },
        backendPort: {
          type: 'integer',
          title: 'Local chart worker port',
          description: 'Loopback port used by the bundled local chart worker.',
          minimum: 1024,
          maximum: 65535,
          default: DEFAULT_BACKEND_PORT
        },
        refreshIntervalSeconds: {
          type: 'integer',
          title: 'Chart refresh interval',
          description: 'How often to check the local chart worker for newly published generations.',
          minimum: 2,
          maximum: 300,
          default: DEFAULT_REFRESH_SECONDS
        }
      }
    }),
    uiSchema: () => ({}),
    start: (settings) => {
      stopRuntime();
      try {
        config = normalizeConfig(settings);
      } catch (error) {
        const message = errorMessage(error);
        reportError(message);
        if (typeof app.setPluginError === 'function') {
          app.setPluginError(message);
        }
        return;
      }
      registerProvider();
      const token = lifecycle;
      setStatus('Starting local progressive chart provider');
      void initialize(token).catch((error) => {
        if (token !== lifecycle) {
          return;
        }
        const message = `Startup failed: ${errorMessage(error)}`;
        reportError(message);
        if (typeof app.setPluginError === 'function') {
          app.setPluginError(message);
        }
      });
    },
    stop: () => {
      stopRuntime();
      charts = new Map();
      setStatus('Stopped');
    },
    signalKApiRoutes: (router) => {
      router.get(
        '/resources/charts/:identifier/generations/:generation/:z/:x/:y',
        async (req, res) => {
          await serveGenerationTile(req, res, config.backendPort);
        }
      );
      router.get('/resources/charts/:identifier', (req, res) => {
        const identifier = String(req.params?.identifier ?? '');
        if (!ID_PATTERN.test(identifier)) {
          res.sendStatus(400);
          return;
        }
        const chart = charts.get(identifier);
        if (!chart) {
          res.sendStatus(404);
          return;
        }
        res.json(toV1Descriptor(chart));
      });
      router.get('/resources/charts', (_req, res) => {
        res.json(currentV1());
      });
      return router;
    },
    registerWithRouter: (router) => {
      router.get('/ui', async (_req, res) => {
        await proxyHtml(res, config.backendPort, '/');
      });
      router.get('/api/status', async (_req, res) => {
        await proxyJsonResponse(res, config.backendPort, '/api/status');
      });
      router.get('/api/noaa/catalog', async (req, res) => {
        let suffix = '';
        try {
          suffix = catalogQuery(req.query);
        } catch (error) {
          sendJsonError(res, 400, errorMessage(error));
          return;
        }
        await proxyJsonResponse(res, config.backendPort, `/api/noaa/catalog${suffix}`);
      });
      router.get('/api/progressive/status', async (_req, res) => {
        await proxyJsonResponse(res, config.backendPort, '/api/progressive/status');
      });
      router.get('/api/jobs', async (_req, res) => {
        await proxyJsonResponse(res, config.backendPort, '/api/jobs');
      });
      router.post('/api/progressive/noaa', async (req, res) => {
        await proxyJsonPost(req, res, config.backendPort, '/api/progressive/noaa');
      });
      for (const action of ['pause', 'resume', 'cancel', 'retry']) {
        router.post(`/api/progressive/charts/:chartId/${action}`, async (req, res) => {
          const chartId = String(req.params?.chartId ?? '');
          if (!ID_PATTERN.test(chartId)) {
            sendJsonError(res, 400, 'Invalid chart identifier');
            return;
          }
          await proxyJsonPost(
            req,
            res,
            config.backendPort,
            `/api/progressive/charts/${encodeURIComponent(chartId)}/${action}`,
            true
          );
        });
      }
      router.post('/api/progressive/charts/:chartId/history/clear', async (req, res) => {
        const chartId = String(req.params?.chartId ?? '');
        if (!ID_PATTERN.test(chartId)) {
          sendJsonError(res, 400, 'Invalid chart identifier');
          return;
        }
        await proxyJsonPost(
          req,
          res,
          config.backendPort,
          `/api/progressive/charts/${encodeURIComponent(chartId)}/history/clear`,
          true
        );
      });
      router.post('/api/progressive/charts/:chartId/delete', async (req, res) => {
        const chartId = String(req.params?.chartId ?? '');
        if (!ID_PATTERN.test(chartId)) {
          sendJsonError(res, 400, 'Invalid chart identifier');
          return;
        }
        await proxyJsonPost(
          req,
          res,
          config.backendPort,
          `/api/progressive/charts/${encodeURIComponent(chartId)}/delete`
        );
        if (res.statusCode >= 200 && res.statusCode < 300) {
          await refreshCharts(true).catch(() => {});
        }
      });
      router.post('/api/jobs/url', async (req, res) => {
        await proxyJsonPost(req, res, config.backendPort, '/api/jobs/url');
      });
      router.post('/api/jobs/:jobId/cancel', async (req, res) => {
        const jobId = String(req.params?.jobId ?? '');
        if (!JOB_ID_PATTERN.test(jobId)) {
          sendJsonError(res, 400, 'Invalid job identifier');
          return;
        }
        await proxyJsonPost(
          req,
          res,
          config.backendPort,
          `/api/jobs/${encodeURIComponent(jobId)}/cancel`,
          true
        );
      });
      router.get('/status', (_req, res) => {
        res.json({
          backend: `${BACKEND_HOST}:${config.backendPort}`,
          localOnly: true,
          childRunning: Boolean(child && child.exitCode === null),
          charts: [...charts.values()].map((chart) => ({
            identifier: chart.identifier,
            generation: chart.generation,
            phase: chart.phase
          }))
        });
      });
      router.post('/refresh', async (_req, res) => {
        try {
          const refreshed = await refreshCharts(true);
          res.json({ refreshed: refreshed.length });
        } catch (error) {
          res.status(502).json({ error: errorMessage(error) });
        }
      });
      return router;
    }
  };

  return plugin;
}

function normalizeConfig(settings) {
  const value = settings && typeof settings === 'object' && !Array.isArray(settings) ? settings : {};
  const allowed = new Set(['startLocalBackend', 'backendPort', 'refreshIntervalSeconds']);
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      throw new Error(`Unsupported plugin setting: ${key}`);
    }
  }
  return {
    startLocalBackend: readBoolean(value.startLocalBackend, true, 'startLocalBackend'),
    backendPort: readInteger(value.backendPort, DEFAULT_BACKEND_PORT, 1024, 65535, 'backendPort'),
    refreshIntervalSeconds: readInteger(
      value.refreshIntervalSeconds,
      DEFAULT_REFRESH_SECONDS,
      2,
      300,
      'refreshIntervalSeconds'
    )
  };
}

function readBoolean(value, fallback, name) {
  if (value === undefined) {
    return fallback;
  }
  if (typeof value !== 'boolean') {
    throw new Error(`${name} must be a boolean`);
  }
  return value;
}

function readInteger(value, fallback, minimum, maximum, name) {
  if (value === undefined) {
    return fallback;
  }
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer from ${minimum} through ${maximum}`);
  }
  return value;
}

function normalizeChart(raw, fallbackIdentifier) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new Error('Chart descriptors must be objects');
  }
  const identifier = String(raw.identifier ?? raw.id ?? fallbackIdentifier ?? '');
  if (!ID_PATTERN.test(identifier)) {
    throw new Error(`Invalid chart identifier: ${identifier}`);
  }
  const generation = normalizeGeneration(raw.generation ?? raw.active_generation);
  const name = boundedString(raw.name ?? identifier, 1, 160, 'chart name');
  const description = boundedString(raw.description ?? '', 0, 1_000, 'chart description');
  const bounds = normalizeBounds(raw.bounds);
  const minzoom = strictInteger(raw.minzoom, 0, MAX_ZOOM, 'minzoom');
  const maxzoom = strictInteger(raw.maxzoom, minzoom, MAX_ZOOM, 'maxzoom');
  const format = String(raw.format ?? 'pbf').toLowerCase();
  if (!['pbf', 'png', 'jpg', 'jpeg', 'webp'].includes(format)) {
    throw new Error(`Unsupported chart format: ${format}`);
  }
  const type = boundedString(raw.type ?? (format === 'pbf' ? 'S-57' : 'tilelayer'), 1, 40, 'type');
  const scale = raw.scale === undefined ? 250_000 : strictInteger(raw.scale, 1, 1_000_000_000, 'scale');
  const phase = raw.phase === 'refined' ? 'refined' : 'preview';
  const layers = normalizeLayers(raw.layers ?? raw.chartLayers ?? raw.vector_layers ?? []);
  return {
    identifier,
    generation,
    name,
    description,
    bounds,
    minzoom,
    maxzoom,
    format: format === 'jpeg' ? 'jpg' : format,
    type,
    scale,
    phase,
    layers
  };
}

function normalizeGeneration(value) {
  if (Number.isInteger(value) && value >= 0 && value <= 999_999_999_999) {
    return `g${String(value).padStart(6, '0')}`;
  }
  const text = String(value ?? '');
  const normalized = /^[0-9]{1,12}$/.test(text) ? `g${text}` : text;
  if (!GENERATION_PATTERN.test(normalized)) {
    throw new Error(`Invalid chart generation: ${text}`);
  }
  return normalized;
}

function normalizeBounds(value) {
  if (!Array.isArray(value) || value.length !== 4 || value.some((part) => !Number.isFinite(part))) {
    throw new Error('Chart bounds must contain four finite numbers');
  }
  const [west, south, east, north] = value;
  if (
    west < -180 ||
    west > 180 ||
    east < -180 ||
    east > 180 ||
    south < -90 ||
    south > 90 ||
    north < -90 ||
    north > 90 ||
    south > north ||
    west > east
  ) {
    throw new Error('Chart bounds are outside geographic limits or reversed');
  }
  return [west, south, east, north];
}

function normalizeLayers(value) {
  if (!Array.isArray(value) || value.length > 512) {
    throw new Error('Chart layers must be an array with at most 512 entries');
  }
  const result = [];
  for (const entry of value) {
    const layer = typeof entry === 'object' && entry !== null ? entry.id : entry;
    if (typeof layer !== 'string' || !LAYER_PATTERN.test(layer)) {
      throw new Error(`Invalid chart layer: ${String(layer)}`);
    }
    if (!result.includes(layer)) {
      result.push(layer);
    }
  }
  return result;
}

function strictInteger(value, minimum, maximum, name) {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer from ${minimum} through ${maximum}`);
  }
  return value;
}

function boundedString(value, minimum, maximum, name) {
  if (typeof value !== 'string' || value.length < minimum || value.length > maximum) {
    throw new Error(`${name} must contain from ${minimum} through ${maximum} characters`);
  }
  return value;
}

function tileUrl(chart) {
  return `${TILE_PATH}/${encodeURIComponent(chart.identifier)}/generations/${chart.generation}/{z}/{x}/{y}`;
}

function commonDescriptor(chart) {
  return {
    identifier: chart.identifier,
    name: chart.name,
    description: chart.description,
    bounds: [...chart.bounds],
    minzoom: chart.minzoom,
    maxzoom: chart.maxzoom,
    format: chart.format,
    type: chart.type,
    scale: chart.scale,
    generation: chart.generation,
    progressivePhase: chart.phase
  };
}

function toV1Descriptor(chart) {
  return {
    ...commonDescriptor(chart),
    tilemapUrl: tileUrl(chart),
    chartLayers: [...chart.layers]
  };
}

function toV2Descriptor(chart) {
  return {
    ...commonDescriptor(chart),
    url: tileUrl(chart),
    layers: [...chart.layers]
  };
}

function descriptorFingerprint(chart) {
  return JSON.stringify(toV2Descriptor(chart));
}

async function fetchChartSnapshot(port) {
  let response = null;
  for (const descriptorPath of DESCRIPTOR_PATHS) {
    response = await requestLocal(port, descriptorPath, {
      limit: MAX_DESCRIPTOR_BYTES,
      timeoutMs: 5_000,
      accept: 'application/json'
    });
    if (response.status !== 404) {
      break;
    }
  }
  if (!response || response.status !== 200) {
    throw new Error(`Chart descriptor endpoint returned HTTP ${response?.status ?? 502}`);
  }

  let payload;
  try {
    payload = JSON.parse(response.body.toString('utf8'));
  } catch {
    throw new Error('Chart descriptor endpoint returned invalid JSON');
  }

  let entries;
  if (Array.isArray(payload)) {
    entries = payload.map((value) => [undefined, value]);
  } else if (payload && typeof payload === 'object' && Array.isArray(payload.charts)) {
    entries = payload.charts.map((value) => [undefined, value]);
  } else if (payload && typeof payload === 'object' && payload.charts && typeof payload.charts === 'object') {
    entries = Object.entries(payload.charts);
  } else if (payload && typeof payload === 'object') {
    entries = Object.entries(payload);
  } else {
    throw new Error('Chart descriptor response has an unsupported shape');
  }

  if (entries.length > MAX_CHARTS) {
    throw new Error(`Chart descriptor response exceeds ${MAX_CHARTS} charts`);
  }
  const seen = new Set();
  const charts = entries.map(([identifier, raw]) => {
    const chart = normalizeChart(raw, identifier);
    if (seen.has(chart.identifier)) {
      throw new Error(`Duplicate chart identifier: ${chart.identifier}`);
    }
    seen.add(chart.identifier);
    return chart;
  });
  return charts.sort((a, b) => a.identifier.localeCompare(b.identifier));
}

async function backendIsReachable(port) {
  try {
    const response = await requestLocal(port, '/api/status', {
      limit: 64 * 1024,
      timeoutMs: 1_500,
      accept: 'application/json'
    });
    return response.status === 200;
  } catch {
    return false;
  }
}

async function waitForBackend(port, timeoutMs, cancelled) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline && !cancelled()) {
    if (await backendIsReachable(port)) {
      return true;
    }
    await delay(250);
  }
  return false;
}

async function serveGenerationTile(req, res, port) {
  const params = req.params ?? {};
  const identifier = String(params.identifier ?? '');
  const generation = String(params.generation ?? '');
  const z = parseCoordinate(params.z);
  const x = parseCoordinate(params.x);
  const y = parseCoordinate(params.y);
  if (
    !ID_PATTERN.test(identifier) ||
    !GENERATION_PATTERN.test(generation) ||
    z === null ||
    x === null ||
    y === null ||
    z > MAX_ZOOM ||
    x >= 2 ** z ||
    y >= 2 ** z
  ) {
    res.sendStatus(400);
    return;
  }

  const backendPath = `/api/provider/charts/${encodeURIComponent(identifier)}/${generation}/${z}/${x}/${y}`;
  let response;
  try {
    response = await requestLocal(port, backendPath, {
      limit: MAX_TILE_BYTES,
      timeoutMs: 30_000,
      accept: 'application/x-protobuf,application/vnd.mapbox-vector-tile,image/*'
    });
  } catch (error) {
    res.status(502).send(errorMessage(error));
    return;
  }

  if (response.status === 404) {
    res.sendStatus(404);
    return;
  }
  if (response.status === 409 || response.status === 425 || response.status === 503) {
    res.set?.('Retry-After', safeRetryAfter(response.headers['retry-after']));
    res.sendStatus(503);
    return;
  }
  if (response.status !== 200) {
    res.sendStatus(502);
    return;
  }

  const contentType = safeTileContentType(response.headers['content-type']);
  if (!contentType) {
    res.status(502).send('Chart Baker returned an unsupported tile content type');
    return;
  }
  const headers = {
    'Content-Type': contentType,
    'Content-Length': String(response.body.length),
    'Cache-Control': 'public, max-age=31536000, immutable'
  };
  if (response.headers['content-encoding'] === 'gzip') {
    headers['Content-Encoding'] = 'gzip';
  }
  const etag = response.headers.etag;
  if (typeof etag === 'string' && /^(W\/)?"[A-Za-z0-9._:-]{1,160}"$/.test(etag)) {
    headers.ETag = etag;
  }
  res.writeHead(200, headers);
  res.end(response.body);
}

function parseCoordinate(value) {
  const text = String(value ?? '');
  if (!/^(0|[1-9][0-9]{0,9})$/.test(text)) {
    return null;
  }
  const result = Number(text);
  return Number.isSafeInteger(result) ? result : null;
}

function safeTileContentType(value) {
  const type = String(value ?? '').split(';', 1)[0].trim().toLowerCase();
  const allowed = new Set([
    'application/x-protobuf',
    'application/vnd.mapbox-vector-tile',
    'application/octet-stream',
    'image/png',
    'image/jpeg',
    'image/webp'
  ]);
  return allowed.has(type) ? type : null;
}

function safeRetryAfter(value) {
  const text = String(value ?? '1');
  return /^[1-9][0-9]{0,2}$/.test(text) ? text : '1';
}

function requestLocal(port, requestPath, options) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (callback, value) => {
      if (settled) {
        return;
      }
      settled = true;
      callback(value);
    };
    const request = http.request(
      {
        host: BACKEND_HOST,
        port,
        path: requestPath,
        method: options.method ?? 'GET',
        agent: false,
        headers: {
          Accept: options.accept,
          Connection: 'close',
          ...(options.body
            ? {
                'Content-Type': 'application/json',
                'Content-Length': String(options.body.length)
              }
            : {})
        }
      },
      (response) => {
        const declared = Number(response.headers['content-length']);
        if (Number.isFinite(declared) && declared > options.limit) {
          response.destroy();
          finish(reject, new Error(`Local response exceeds ${options.limit} bytes`));
          return;
        }
        const chunks = [];
        let total = 0;
        response.on('data', (chunk) => {
          total += chunk.length;
          if (total > options.limit) {
            response.destroy();
            finish(reject, new Error(`Local response exceeds ${options.limit} bytes`));
            return;
          }
          chunks.push(chunk);
        });
        response.on('end', () => {
          finish(resolve, {
            status: response.statusCode ?? 502,
            headers: response.headers,
            body: Buffer.concat(chunks, total)
          });
        });
        response.on('error', (error) => finish(reject, error));
      }
    );
    request.setTimeout(options.timeoutMs, () => {
      request.destroy(new Error('Local Chart Baker request timed out'));
    });
    request.on('error', (error) => finish(reject, error));
    request.end(options.body);
  });
}

async function proxyHtml(res, port, requestPath) {
  let response;
  try {
    response = await requestLocal(port, requestPath, {
      limit: MAX_UI_BYTES,
      timeoutMs: 10_000,
      accept: 'text/html'
    });
  } catch (error) {
    sendJsonError(res, 502, errorMessage(error));
    return;
  }
  if (response.status !== 200) {
    sendJsonError(res, response.status >= 400 && response.status < 500 ? response.status : 502, 'Chart Baker UI is unavailable');
    return;
  }
  if (mediaType(response.headers['content-type']) !== 'text/html') {
    sendJsonError(res, 502, 'Chart Baker returned an unsupported UI content type');
    return;
  }
  res.writeHead(200, {
    'Content-Type': 'text/html; charset=utf-8',
    'Content-Length': String(response.body.length),
    'Cache-Control': 'no-store'
  });
  res.end(response.body);
}

async function proxyJsonResponse(res, port, requestPath, requestOptions = {}) {
  let response;
  try {
    response = await requestLocal(port, requestPath, {
      limit: MAX_API_RESPONSE_BYTES,
      timeoutMs: requestOptions.timeoutMs ?? 30_000,
      accept: 'application/json',
      method: requestOptions.method,
      body: requestOptions.body
    });
  } catch (error) {
    sendJsonError(res, 502, errorMessage(error));
    return;
  }
  if (mediaType(response.headers['content-type']) !== 'application/json') {
    sendJsonError(res, 502, 'Chart Baker returned an unsupported API content type');
    return;
  }
  const status = response.status >= 200 && response.status < 500 ? response.status : 502;
  if (status === 502 && response.status !== 502) {
    sendJsonError(res, 502, `Chart Baker returned HTTP ${response.status}`);
    return;
  }
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': String(response.body.length),
    'Cache-Control': 'no-store'
  });
  res.end(response.body);
}

async function proxyJsonPost(req, res, port, requestPath, allowEmpty = false) {
  let body;
  try {
    body = await readJsonBody(req, allowEmpty);
  } catch (error) {
    sendJsonError(res, error.statusCode ?? 400, errorMessage(error));
    return;
  }
  await proxyJsonResponse(res, port, requestPath, {
    method: 'POST',
    body,
    timeoutMs: 30_000
  });
}

async function readJsonBody(req, allowEmpty) {
  const contentType = mediaType(req.headers?.['content-type'] ?? req.get?.('content-type'));
  if (contentType !== 'application/json') {
    if (allowEmpty && contentType === '' && req.body === undefined) {
      return Buffer.from('{}');
    }
    throw inputError(415, 'Content-Type must be application/json');
  }
  const declaredText = req.headers?.['content-length'];
  if (declaredText !== undefined) {
    if (!/^[0-9]+$/.test(String(declaredText))) {
      throw inputError(400, 'Invalid Content-Length');
    }
    if (Number(declaredText) > MAX_POST_BODY_BYTES) {
      throw inputError(413, `JSON body exceeds ${MAX_POST_BODY_BYTES} bytes`);
    }
  }

  let bytes;
  if (req.body !== undefined) {
    try {
      bytes = Buffer.from(JSON.stringify(req.body));
    } catch {
      throw inputError(400, 'JSON body is not serializable');
    }
  } else {
    const chunks = [];
    let total = 0;
    for await (const chunk of req) {
      const bytesChunk = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      total += bytesChunk.length;
      if (total > MAX_POST_BODY_BYTES) {
        throw inputError(413, `JSON body exceeds ${MAX_POST_BODY_BYTES} bytes`);
      }
      chunks.push(bytesChunk);
    }
    bytes = Buffer.concat(chunks, total);
  }
  if (bytes.length > MAX_POST_BODY_BYTES) {
    throw inputError(413, `JSON body exceeds ${MAX_POST_BODY_BYTES} bytes`);
  }
  if (allowEmpty && bytes.length === 0) {
    return Buffer.from('{}');
  }
  let parsed;
  try {
    parsed = JSON.parse(bytes.toString('utf8'));
  } catch {
    throw inputError(400, 'Request body must contain valid JSON');
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw inputError(400, 'Request body must be a JSON object');
  }
  return Buffer.from(JSON.stringify(parsed));
}

function catalogQuery(query) {
  if (query === undefined || query === null || Object.keys(query).length === 0) {
    return '';
  }
  if (Object.keys(query).some((key) => key !== 'refresh')) {
    throw new Error('Unsupported catalog query parameter');
  }
  const value = Array.isArray(query.refresh) ? query.refresh[0] : query.refresh;
  if (value === true || value === 'true' || value === '1') {
    return '?refresh=true';
  }
  if (value === false || value === 'false' || value === '0') {
    return '?refresh=false';
  }
  throw new Error('refresh must be true or false');
}

function mediaType(value) {
  return String(value ?? '').split(';', 1)[0].trim().toLowerCase();
}

function inputError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function sendJsonError(res, statusCode, message) {
  const body = Buffer.from(JSON.stringify({ error: message }));
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': String(body.length),
    'Cache-Control': 'no-store'
  });
  res.end(body);
}

function spawnLocalBackend(app, port, debug, reportError) {
  const script = path.join(__dirname, 'chart_baker.py');
  const dataDirectory = path.join(app.getDataDirPath(), 'chart-baker-data');
  debug(`Starting bundled Chart Baker on ${BACKEND_HOST}:${port}`);
  const child = spawn(
    'uv',
    [
      'run',
      script,
      '--host',
      BACKEND_HOST,
      '--port',
      String(port),
      '--data-dir',
      dataDirectory
    ],
    {
      cwd: __dirname,
      env: backendEnvironment(),
      stdio: ['ignore', 'pipe', 'pipe']
    }
  );
  child.stdout.on('data', (chunk) => debug(String(chunk).trim().slice(0, 4_096)));
  child.stderr.on('data', (chunk) => debug(String(chunk).trim().slice(0, 4_096)));
  child.on('error', (error) => reportError(`Unable to start local Chart Baker: ${errorMessage(error)}`));
  child.on('exit', (code, signal) => {
    if (code && code !== 0) {
      reportError(`Local Chart Baker exited with status ${code}${signal ? ` (${signal})` : ''}`);
    }
  });
  return child;
}

function backendEnvironment() {
  const environment = { ...process.env, PYTHONUNBUFFERED: '1' };
  delete environment.LISTEN_FDS;
  delete environment.LISTEN_FDNAMES;
  delete environment.LISTEN_PID;
  if (
    process.platform === 'linux' &&
    !environment.XDG_RUNTIME_DIR &&
    typeof process.getuid === 'function'
  ) {
    environment.XDG_RUNTIME_DIR = `/run/user/${process.getuid()}`;
  }
  return environment;
}

function stopChild(child) {
  if (child.exitCode !== null || child.killed) {
    return;
  }
  child.kill('SIGTERM');
  const timer = setTimeout(() => {
    if (child.exitCode === null) {
      child.kill('SIGKILL');
    }
  }, 5_000);
  timer.unref?.();
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

pluginConstructor._test = {
  backendEnvironment,
  normalizeConfig,
  normalizeChart,
  normalizeGeneration,
  fetchChartSnapshot,
  serveGenerationTile,
  toV1Descriptor,
  toV2Descriptor,
  requestLocal,
  readJsonBody,
  catalogQuery,
  proxyJsonResponse
};

module.exports = pluginConstructor;
