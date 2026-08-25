'use strict';

const assert = require('node:assert/strict');
const http = require('node:http');
const test = require('node:test');

const createPlugin = require('./index.js');
const bridge = createPlugin._test;

const SAMPLE_CHART = {
  identifier: 'noaa-sf-bay',
  generation: 'g000007',
  name: 'NOAA ENC San Francisco Bay',
  description: 'Progressive local chart',
  bounds: [-123, 37, -121, 39],
  minzoom: 8,
  maxzoom: 14,
  format: 'pbf',
  type: 'S-57',
  scale: 25_000,
  phase: 'preview',
  layers: ['DEPARE', 'LNDARE', 'SOUNDG']
};

test('configuration is loopback-only and strictly validated', () => {
  const previousRuntimeDirectory = process.env.XDG_RUNTIME_DIR;
  process.env.XDG_RUNTIME_DIR = '/run/user/test';
  assert.equal(bridge.backendEnvironment().XDG_RUNTIME_DIR, '/run/user/test');
  assert.equal(bridge.backendEnvironment().PYTHONUNBUFFERED, '1');
  if (previousRuntimeDirectory === undefined) {
    delete process.env.XDG_RUNTIME_DIR;
  } else {
    process.env.XDG_RUNTIME_DIR = previousRuntimeDirectory;
  }
  assert.deepEqual(bridge.normalizeConfig({}), {
    startLocalBackend: true,
    backendPort: 8787,
    refreshIntervalSeconds: 3
  });
  assert.throws(
    () => bridge.normalizeConfig({ backendHost: 'chart-worker.example' }),
    /Unsupported plugin setting/
  );
  assert.throws(() => bridge.normalizeConfig({ backendPort: 80 }), /backendPort/);
  assert.throws(
    () => bridge.normalizeConfig({ refreshIntervalSeconds: '2' }),
    /refreshIntervalSeconds/
  );
});

test('chart descriptors receive immutable generation URLs', () => {
  const chart = bridge.normalizeChart({ ...SAMPLE_CHART, generation: 7 });
  assert.equal(chart.generation, 'g000007');
  const v1 = bridge.toV1Descriptor(chart);
  const v2 = bridge.toV2Descriptor(chart);
  const expected =
    '/signalk/v1/api/resources/charts/noaa-sf-bay/generations/g000007/{z}/{x}/{y}';
  assert.equal(v1.tilemapUrl, expected);
  assert.equal(v2.url, expected);
  assert.equal(bridge.normalizeGeneration('g000008-refined'), 'g000008-refined');
  assert.deepEqual(v1.chartLayers, SAMPLE_CHART.layers);
  assert.deepEqual(v2.layers, SAMPLE_CHART.layers);
  assert.throws(
    () => bridge.normalizeChart({ ...SAMPLE_CHART, generation: '../current' }),
    /generation/
  );
  assert.throws(
    () => bridge.normalizeChart({ ...SAMPLE_CHART, identifier: '../chart' }),
    /identifier/
  );
  assert.throws(
    () => bridge.normalizeChart({ ...SAMPLE_CHART, layers: ['DEPARE', 'bad layer'] }),
    /layer/
  );
});

test('plugin refreshes resources, emits deltas, and proxies bounded generation tiles', async (t) => {
  const tile = Buffer.from([0x1f, 0x8b, 0x08, 0x00, 0x01, 0x02, 0x03]);
  let descriptors = [SAMPLE_CHART];
  let tileRequests = 0;
  const managementRequests = [];
  const backend = http.createServer(async (req, res) => {
    if (req.url === '/') {
      const body = Buffer.from('<!doctype html><title>Charts Provider Progressive</title>');
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Content-Length': String(body.length)
      });
      res.end(body);
      return;
    }
    if (req.url === '/api/status') {
      sendJson(res, { version: 'test' });
      return;
    }
    if (req.url === '/api/noaa/catalog?refresh=true') {
      sendJson(res, { entries: [{ chart_id: 'US4CA123' }] });
      return;
    }
    if (req.method === 'POST' && req.url === '/api/progressive/noaa') {
      const body = JSON.parse((await readRequest(req)).toString('utf8'));
      managementRequests.push({ path: req.url, body });
      sendJson(res, { chart_id: 'noaa-sf-bay' }, 202);
      return;
    }
    if (req.method === 'POST' && req.url === '/api/jobs/abc123/cancel') {
      const body = JSON.parse((await readRequest(req)).toString('utf8'));
      managementRequests.push({ path: req.url, body });
      sendJson(res, { cancelled: true }, 202);
      return;
    }
    if (req.url === '/api/provider/charts') {
      sendJson(res, { charts: descriptors });
      return;
    }
    if (req.url === '/api/provider/charts/noaa-sf-bay/g000007/8/40/98') {
      tileRequests += 1;
      res.writeHead(200, {
        'Content-Type': 'application/x-protobuf',
        'Content-Encoding': 'gzip',
        ETag: '"g000007-8-40-98"'
      });
      res.end(tile);
      return;
    }
    res.writeHead(404).end();
  });
  await listen(backend);
  t.after(() => close(backend));

  const app = makeApp();
  const plugin = createPlugin(app);
  const apiRouter = makeRouter();
  const managementRouter = makeRouter();
  plugin.signalKApiRoutes(apiRouter);
  plugin.registerWithRouter(managementRouter);
  plugin.start({
    startLocalBackend: false,
    backendPort: backend.address().port,
    refreshIntervalSeconds: 2
  });
  t.after(() => plugin.stop());

  await waitFor(async () => {
    const resources = await app.provider.methods.listResources({});
    return resources['noaa-sf-bay'];
  });
  const resources = await app.provider.methods.listResources({});
  assert.equal(resources['noaa-sf-bay'].generation, 'g000007');
  assert.match(resources['noaa-sf-bay'].url, /g000007/);
  assert.ok(
    app.messages.some(
      (entry) =>
        entry.message.updates[0].values[0].path === 'resources.charts.noaa-sf-bay' &&
        entry.version === 2
    )
  );

  const uiRoute = managementRouter.gets.find((route) => route.path === '/ui');
  const uiResponse = makeResponse();
  await uiRoute.handler({}, uiResponse);
  assert.equal(uiResponse.statusCode, 200);
  assert.equal(uiResponse.headers['Content-Type'], 'text/html; charset=utf-8');
  assert.match(uiResponse.body.toString('utf8'), /Charts Provider Progressive/);

  const catalogRoute = managementRouter.gets.find(
    (route) => route.path === '/api/noaa/catalog'
  );
  const catalogResponse = makeResponse();
  await catalogRoute.handler({ query: { refresh: 'true' } }, catalogResponse);
  assert.equal(catalogResponse.statusCode, 200);
  assert.deepEqual(JSON.parse(catalogResponse.body.toString('utf8')), {
    entries: [{ chart_id: 'US4CA123' }]
  });

  const progressiveRoute = managementRouter.posts.find(
    (route) => route.path === '/api/progressive/noaa'
  );
  const progressiveResponse = makeResponse();
  await progressiveRoute.handler(
    {
      headers: { 'content-type': 'application/json' },
      body: { name: 'Bay', chart_ids: ['US4CA123'] }
    },
    progressiveResponse
  );
  assert.equal(progressiveResponse.statusCode, 202);
  assert.deepEqual(managementRequests.at(-1), {
    path: '/api/progressive/noaa',
    body: { name: 'Bay', chart_ids: ['US4CA123'] }
  });

  const wrongTypeResponse = makeResponse();
  await progressiveRoute.handler(
    { headers: { 'content-type': 'text/plain' }, body: { name: 'Bay' } },
    wrongTypeResponse
  );
  assert.equal(wrongTypeResponse.statusCode, 415);

  const largeBodyResponse = makeResponse();
  await progressiveRoute.handler(
    {
      headers: { 'content-type': 'application/json' },
      body: { value: 'x'.repeat(512 * 1024) }
    },
    largeBodyResponse
  );
  assert.equal(largeBodyResponse.statusCode, 413);

  const cancelRoute = managementRouter.posts.find(
    (route) => route.path === '/api/jobs/:jobId/cancel'
  );
  const cancelResponse = makeResponse();
  await cancelRoute.handler(
    { params: { jobId: 'abc123' }, headers: {}, body: undefined },
    cancelResponse
  );
  assert.equal(cancelResponse.statusCode, 202);
  assert.deepEqual(managementRequests.at(-1), {
    path: '/api/jobs/abc123/cancel',
    body: {}
  });

  const tileRoute = apiRouter.gets.find((route) => route.path.includes(':generation'));
  const tileResponse = makeResponse();
  await tileRoute.handler(
    {
      params: {
        identifier: 'noaa-sf-bay',
        generation: 'g000007',
        z: '8',
        x: '40',
        y: '98'
      }
    },
    tileResponse
  );
  assert.equal(tileResponse.statusCode, 200);
  assert.equal(tileResponse.headers['Cache-Control'], 'public, max-age=31536000, immutable');
  assert.equal(tileResponse.headers['Content-Encoding'], 'gzip');
  assert.deepEqual(tileResponse.body, tile);
  assert.equal(tileRequests, 1);

  const invalidResponse = makeResponse();
  await tileRoute.handler(
    {
      params: {
        identifier: 'noaa-sf-bay',
        generation: '../live',
        z: '8',
        x: '40',
        y: '98'
      }
    },
    invalidResponse
  );
  assert.equal(invalidResponse.statusCode, 400);
  assert.equal(tileRequests, 1);

  descriptors = [{ ...SAMPLE_CHART, generation: 'g000008', phase: 'refined' }];
  const refreshRoute = managementRouter.posts.find((route) => route.path === '/refresh');
  const refreshResponse = makeResponse();
  await refreshRoute.handler({}, refreshResponse);
  assert.equal(refreshResponse.jsonBody.refreshed, 1);
  const updated = await app.provider.methods.getResource('noaa-sf-bay');
  assert.equal(updated.generation, 'g000008');
  assert.equal(updated.progressivePhase, 'refined');
  assert.ok(
    app.messages.some(
      (entry) => entry.message.updates[0].values[0].value?.generation === 'g000008'
    )
  );

  descriptors = [];
  await refreshRoute.handler({}, makeResponse());
  await assert.rejects(app.provider.methods.getResource('noaa-sf-bay'), /not found/i);
  assert.ok(
    app.messages.some(
      (entry) => entry.message.updates[0].values[0].value === null
    )
  );
});

test('descriptor and tile response limits fail closed', async (t) => {
  const backend = http.createServer((req, res) => {
    if (req.url === '/api/provider/charts') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(Buffer.alloc(2 * 1024 * 1024 + 1, 0x20));
      return;
    }
    if (req.url?.includes('/api/provider/charts/')) {
      res.writeHead(200, { 'Content-Type': 'application/x-protobuf' });
      res.end(Buffer.alloc(16 * 1024 * 1024 + 1));
      return;
    }
    res.writeHead(404).end();
  });
  await listen(backend);
  t.after(() => close(backend));

  await assert.rejects(bridge.fetchChartSnapshot(backend.address().port), /exceeds/);
  const response = makeResponse();
  await bridge.serveGenerationTile(
    {
      params: {
        identifier: 'noaa-sf-bay',
        generation: 'g000001',
        z: '1',
        x: '0',
        y: '0'
      }
    },
    response,
    backend.address().port
  );
  assert.equal(response.statusCode, 502);
});

function makeApp() {
  return {
    config: { version: '2.24.0' },
    messages: [],
    statuses: [],
    errors: [],
    provider: null,
    debug() {},
    error(message) {
      this.errors.push(message);
    },
    setPluginStatus(message) {
      this.statuses.push(message);
    },
    setPluginError(message) {
      this.errors.push(message);
    },
    getDataDirPath() {
      return '/tmp/signalk-progressive-test';
    },
    registerResourceProvider(provider) {
      this.provider = provider;
    },
    handleMessage(pluginId, message, version) {
      this.messages.push({ pluginId, message, version });
    }
  };
}

function makeRouter() {
  return {
    gets: [],
    posts: [],
    get(path, handler) {
      this.gets.push({ path, handler });
    },
    post(path, handler) {
      this.posts.push({ path, handler });
    }
  };
}

function makeResponse() {
  return {
    statusCode: 200,
    headers: {},
    body: Buffer.alloc(0),
    jsonBody: undefined,
    status(code) {
      this.statusCode = code;
      return this;
    },
    set(name, value) {
      this.headers[name] = value;
      return this;
    },
    json(value) {
      this.jsonBody = value;
      return this;
    },
    send(value = '') {
      this.body = Buffer.isBuffer(value) ? value : Buffer.from(String(value));
      return this;
    },
    sendStatus(code) {
      this.statusCode = code;
      this.body = Buffer.from(String(code));
      return this;
    },
    writeHead(code, headers = {}) {
      this.statusCode = code;
      this.headers = { ...this.headers, ...headers };
      return this;
    },
    end(value = Buffer.alloc(0)) {
      this.body = Buffer.isBuffer(value) ? value : Buffer.from(String(value));
      return this;
    }
  };
}

function sendJson(res, value, status = 200) {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Content-Length': String(body.length)
  });
  res.end(body);
}

function readRequest(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
}

function close(server) {
  return new Promise((resolve) => server.close(resolve));
}

async function waitFor(check) {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    if (await check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error('Timed out waiting for plugin state');
}
