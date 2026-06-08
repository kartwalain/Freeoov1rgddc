// ═══════════════════════════════════════════════════════════════════════
// FreeOO AI PRO v1.0 — Local Proxy Server (server.js) · Upstox API v2+v3
// ═══════════════════════════════════════════════════════════════════════
//
// PURPOSE:
//   This server runs ALL Upstox API tasks server-side.
//   The browser (HTML file) only handles display — zero API calls from browser.
//
// API VERSION MAPPING (per Upstox official docs v3):
//   v2: /market-quote/quotes  (full market quotes)
//   v2: /option/chain         (put/call option chain)
//   v3: /market-quote/ltp     (LTP lightweight quotes)
//   v3: /market-quote/ohlc    (OHLC quotes)
//   v3: /historical-candle/   (historical + intraday candles)
//   v3: /feed/market-data-feed/authorize  (WebSocket auth — one URL per WS conn)
//   v3: /market-information/exchange-status (market status)
//
// UPSTOX PLUS MEMBER — 5 WebSocket Connections:
//   Upstox allows 5 WS connections for Plus members (vs 2 standard).
//   This app uses a 5-WS Pool for optimal, restriction-free real-time data:
//
//   WS1 (INDEX_LTPC)    mode=ltpc         keys≤6    → 5000-key individual limit
//   WS2 (OPT_CURR)      mode=option_greeks keys≤400  → 3000-key individual limit
//   WS3 (OPT_NEXT)      mode=option_greeks keys≤400  → 3000-key individual limit
//   WS4 (FULL_D30_PLUS) mode=full_d30      keys≤50   → Plus-exclusive mode
//   WS5 (OPT_EXTRA)     mode=option_greeks keys≤400  → additional symbols/BankNifty
//
//   Each WS connection gets its own authorized URL via /api/ws-auth?connIdx=0-4
//   Each reconnects independently — one drop does NOT affect others.
//   Combined subscriptions across all 5 connections stay within all Upstox limits.
//   Zero risk of 429 from WebSocket (WS is unlimited ticks, just sub-count limited).
//
// HOW IT WORKS:
//   1. Browser opens index.html (served from this server or opened directly)
//   2. Browser calls  GET /api/upstox?url=<encoded-upstox-url>&token=<TOKEN>
//   3. This server fetches from Upstox v3 API with Authorization header (no CORS issue)
//   4. Returns raw Upstox JSON to browser
//   5. Browser just renders the data — no proxy gymnastics needed
//   6. For WS: browser calls /api/ws-auth?connIdx=0 through ?connIdx=4 for 5 connections
//
// USAGE:
//   node server.js
//   → Opens on http://localhost:3000
//   → Open browser to http://localhost:3000/index.html
//
// REQUIREMENTS:
//   Node.js 18+ (built-in fetch — no npm install needed)
//   OR  Node.js 14-17 with:  npm install node-fetch
//
// GITHUB PAGES:
//   For GitHub Pages, use freeoo-sw.js (Service Worker) instead.
//   The SW is auto-registered by the HTML file — no server needed.
//   Deploy both index.html AND freeoo-sw.js to your repo root.
//
// v1.0 CHANGES:
//   - Version updated to v1.0 across all 3 files (index.html · server.js · freeoo-sw.js)
//   - [FIX] WS stale handler guard: onclose/onerror/onmessage/ping only act on current WS
//   - [FIX] WS subscriptions sent as binary frames (Upstox v3 docs requirement)
//   - [FIX v1611] Chart historical↔real-time flicker: split setData always in liveStreaming
//   - [FIX v1611] Option chain 30s delay: WS subscription refresh 30s→5s
//   - [FIX v1611] Rate limit: added min 5s REST floor even when WS not live
//   - [VERIFIED] Upstox rate limits: 50/sec · 500/min · 2000/30min (standard APIs)
//   - All prior fixes retained
//
// CONNECTIVITY FIXES:
//   - WS auth: 3 retries with progressive delays 0/300ms/600ms (was 2/fixed 500ms)
//   - WS auth timeout: 10s (was 8s) — auth endpoint slower on mobile/slow connections
//   - Keep-alive timeouts: 75s/76s (unchanged — right for proxy persistence)
//   - Health response updated with new feature list reflecting all connectivity fixes
// ═══════════════════════════════════════════════════════════════════════

'use strict';

const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const url   = require('url');

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || 'localhost';

// ── CORS headers — allow the HTML file to call this server from any origin ──
const CORS_HEADERS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization, Accept, Content-Type',
  'Access-Control-Max-Age':       '86400',
  'Cache-Control':                'no-store, no-cache, must-revalidate',
  'Pragma':                       'no-cache'
};

// ── Allowed Upstox base URLs (whitelist for security — v2 + v3) ──
const ALLOWED_BASES = [
  'https://api.upstox.com/v2/',
  'https://api.upstox.com/v3/'
];

// ── API timeout — 6s for REST calls (was 8s — 6s catches dead connections faster on mobile) ──
const API_TIMEOUT_MS = 6000;

// ── WS Auth timeout — 10s (auth involves a redirect; slower on mobile connections) ──
const WS_AUTH_TIMEOUT_MS = 10000;

// ═══════════════════════════════════════════════════════════════════════
// v1.0 RATE LIMITER — server-side guard mirrors Upstox official limits:
//   Standard APIs: 50/sec · 500/min · 2000/30min  (per official docs)
//   Safe budgets:  42/sec · 420/min · 1800/30min  (12% buffer below limits)
//   Note: WebSocket subscriptions do NOT count toward REST rate limits.
//   Note: WS ticks are unlimited — only subscription count is limited.
// ═══════════════════════════════════════════════════════════════════════
const _rl = {
  _calls: [],
  _pauseUntil: 0,
  MAX_PER_SEC:    42,  // safe: 50/sec  -16% buffer
  MAX_PER_MIN:   420,  // safe: 500/min -16% buffer
  MAX_PER_30MIN:1800,  // safe: 2000/30min -10% buffer (BINDING LIMIT per Upstox docs)

  canCall() {
    const now = Date.now();
    if(now < this._pauseUntil) return false;
    this._calls = this._calls.filter(t => now - t < 1800000);
    if(this._calls.length >= this.MAX_PER_30MIN) return false;
    const per60s = this._calls.filter(t => now - t < 60000).length;
    if(per60s >= this.MAX_PER_MIN) return false;
    const per1s = this._calls.filter(t => now - t < 1000).length;
    if(per1s >= this.MAX_PER_SEC) return false;
    return true;
  },

  record() {
    this._calls.push(Date.now());
    if(this._calls.length > 2500) this._calls = this._calls.slice(-2000);
  },

  on429(retryAfterSec) {
    const base = retryAfterSec
      ? retryAfterSec * 1000
      : Math.min(5000 * Math.pow(2, Math.max(0, this._calls.length / 100 - 1)), 60000);
    this._pauseUntil = Date.now() + base;
    this._calls = [];
    const secs = Math.round(base / 1000);
    console.warn(`[v1.0 RateLimit] 429 received — pausing all Upstox REST calls for ${secs}s (WS unaffected)`);
    return secs;
  },

  budget30m() {
    const now = Date.now();
    const used = this._calls.filter(t => now - t < 1800000).length;
    return Math.max(0, this.MAX_PER_30MIN - used);
  },

  usedPerMin() {
    const now = Date.now();
    return this._calls.filter(t => now - t < 60000).length;
  }
};

// ── Fetch implementation (Node 18+ has built-in fetch) ──
let nodeFetch;
if(typeof fetch !== 'undefined'){
  nodeFetch = fetch;
} else {
  try{
    nodeFetch = require('node-fetch');
  }catch(e){
    console.error('ERROR: Node.js 18+ required for built-in fetch, or run: npm install node-fetch');
    process.exit(1);
  }
}

// ── Mime types for static file serving ──
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.json': 'application/json',
  '.ico':  'image/x-icon',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.svg':  'image/svg+xml',
};

// ═══════════════════════════════════════════════════════════════════════
// REQUEST HANDLER
// ═══════════════════════════════════════════════════════════════════════
async function handler(req, res){
  const parsedUrl = url.parse(req.url, true);
  const pathname  = parsedUrl.pathname;

  // ── CORS preflight ──
  if(req.method === 'OPTIONS'){
    res.writeHead(204, CORS_HEADERS);
    res.end();
    return;
  }

  // ── Health check — includes rate budget + WS pool info ──
  if(pathname === '/api/health'){
    res.writeHead(200, {...CORS_HEADERS, 'Content-Type': 'application/json'});
    res.end(JSON.stringify({
      status: 'ok',
      server: 'FreeOO-v1.0-LocalProxy-v3API',
      wsPool: {
        memberType: 'Upstox Plus',
        totalConnections: 5,
        limit: '5 connections (Plus)',
        slots: [
          { idx: 0, role: 'INDEX_LTPC',    mode: 'ltpc',          maxKeys: 5000, desc: 'Index prices (Nifty50, SENSEX, VIX, BankNifty...)' },
          { idx: 1, role: 'OPT_CURR',      mode: 'option_greeks', maxKeys: 400,  desc: 'Current expiry option chain ATM±25' },
          { idx: 2, role: 'OPT_NEXT',      mode: 'option_greeks', maxKeys: 400,  desc: 'Next expiry option chain ATM±25' },
          { idx: 3, role: 'FULL_D30_PLUS', mode: 'full_d30',      maxKeys: 50,   desc: 'Full 30-depth (Plus-exclusive) key instruments' },
          { idx: 4, role: 'OPT_EXTRA',     mode: 'option_greeks', maxKeys: 400,  desc: 'BankNifty / additional symbols / overflow' }
        ]
      },
      time: new Date().toISOString(),
      uptime: Math.round(process.uptime()),
      rateBudget30m: _rl.budget30m(),
      rateUsedPerMin: _rl.usedPerMin(),
      features: [
        'ws-auth-per-conn-idx','rest-proxy','parallel-fetch','keepalive',
        '6s-timeout','10s-ws-auth-timeout','server-rate-guard','retry-after-fwd',
        '503-net-hint','ws-auth-retry-3x-200ms-400ms','5-ws-pool','plus-member',
        'full_d30-mode','independent-ws-reconnect','ws-pool-supervisor',
        'zombie-ws-detection','adaptive-stale-12s-8s-6s',
        'adaptive-bypass-3s-2s-1.2s','sub-delay-200ms-400ms',
        'pagehide-pageshow-handlers','freeze-resume-handlers',
        'ws-auth-ttl-25min','heartbeat-1.5s','keepalive-10s','bgSync-12s',
        'first-open-busy-release','auth-min-300ms','supervisor-4s'
      ]
    }));
    return;
  }

  // ── WebSocket Auth endpoint (v3) — supports ?connIdx=0-4 for 5-WS pool ──
  // Each of the 5 WS pool connections calls this with its own connIdx.
  // Upstox authorized_redirect_uri is single-use — each connection MUST get
  // its own unique URL. Sharing one URL across multiple WS clients causes
  // silent data loss on connections 2-5.
  //
  // CONNECTIVITY FIX: 3 retries with progressive delays 0/300ms/600ms (was 2/fixed 500ms).
  // WS_AUTH_TIMEOUT_MS = 10s (was 8s) — auth endpoint can be slower on mobile.
  if(pathname === '/api/ws-auth'){
    const token   = parsedUrl.query.token;
    const connIdx = parsedUrl.query.connIdx || '0'; // WS pool slot index (0-4)
    if(!token){
      res.writeHead(400, {...CORS_HEADERS, 'Content-Type': 'application/json'});
      res.end(JSON.stringify({error:'token required'}));
      return;
    }

    const WS_AUTH_URL = 'https://api.upstox.com/v3/feed/market-data-feed/authorize';
    // v1.0+: Faster progressive retry delays: immediate → 200ms → 400ms (was 0/300/600ms)
    // 3 retries total. Faster delays help mobile clients reconnect within the WS stale window.
    const retryDelays = [0, 200, 400];

    for(let attempt = 0; attempt < retryDelays.length; attempt++){
      if(retryDelays[attempt] > 0){
        await new Promise(r => setTimeout(r, retryDelays[attempt]));
      }
      try{
        const upResp = await nodeFetch(WS_AUTH_URL, {
          method: 'GET',
          headers:{
            'Authorization': `Bearer ${token}`,
            'Accept': 'application/json',
            'User-Agent': `FreeOO-v1.0-Server/100-WS${connIdx}`,
            'Connection': 'keep-alive'
          },
          signal: AbortSignal.timeout(WS_AUTH_TIMEOUT_MS)
        });
        const data = await upResp.json();
        res.writeHead(upResp.status, {
          ...CORS_HEADERS,
          'Content-Type': 'application/json',
          'X-WS-Auth-Attempt': String(attempt + 1)
        });
        res.end(JSON.stringify(data));
        const ts = new Date().toTimeString().slice(0,8);
        console.log(`[${ts}] WS-AUTH connIdx=${connIdx} → ${upResp.status} (attempt ${attempt+1})`);
        return;
      }catch(e){
        const ts = new Date().toTimeString().slice(0,8);
        console.warn(`[${ts}] WS-AUTH attempt ${attempt+1} failed: ${e.message}`);
        if(attempt < retryDelays.length - 1) continue; // retry
        // All retries exhausted
        const isTimeout = e.name === 'TimeoutError' || e.name === 'AbortError';
        const isNetDown = e.code === 'ECONNREFUSED' || e.code === 'ENOTFOUND' || e.code === 'ETIMEDOUT';
        res.writeHead(isNetDown ? 503 : isTimeout ? 504 : 502,
          {...CORS_HEADERS, 'Content-Type': 'application/json'});
        res.end(JSON.stringify({error:'ws-auth fetch failed', detail: e.message, attempt: attempt+1, connIdx}));
        return;
      }
    }
    return;
  }

  // ── Main Upstox proxy endpoint ──
  if(pathname === '/api/upstox'){
    const targetUrl = parsedUrl.query.url;
    const token     = parsedUrl.query.token;

    if(!targetUrl || !token){
      res.writeHead(400, {...CORS_HEADERS, 'Content-Type': 'application/json'});
      res.end(JSON.stringify({error: 'Missing url or token parameter'}));
      return;
    }

    const isAllowed = ALLOWED_BASES.some(base => targetUrl.startsWith(base));
    if(!isAllowed){
      res.writeHead(403, {...CORS_HEADERS, 'Content-Type': 'application/json'});
      res.end(JSON.stringify({error: 'URL not in allowed list'}));
      return;
    }

    // Server-side rate gate — return 429 immediately if budget exhausted
    if(!_rl.canCall()){
      res.writeHead(429, {...CORS_HEADERS, 'Content-Type': 'application/json'});
      res.end(JSON.stringify({
        error: 'Rate limit: server-side budget exhausted',
        retryAfter: Math.ceil(Math.max(0, _rl._pauseUntil - Date.now()) / 1000) || 5
      }));
      return;
    }

    try{
      _rl.record();
      const upstoxRes = await nodeFetch(targetUrl, {
        method:  'GET',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Accept':        'application/json',
          'User-Agent':    'FreeOO-v1.0-Server/100',
          'Connection':    'keep-alive'
        },
        signal: AbortSignal.timeout(API_TIMEOUT_MS)
      });

      const body = await upstoxRes.text();

      // Forward Retry-After from Upstox 429 responses
      const extraHeaders = {};
      if(upstoxRes.status === 429){
        const ra = upstoxRes.headers.get('retry-after') || upstoxRes.headers.get('Retry-After');
        if(ra){
          extraHeaders['X-Retry-After'] = ra;
          _rl.on429(parseFloat(ra));
        } else {
          _rl.on429(null);
        }
      }

      res.writeHead(upstoxRes.status, {
        ...CORS_HEADERS,
        ...extraHeaders,
        'Content-Type': upstoxRes.headers.get('content-type') || 'application/json'
      });
      res.end(body);

      const ts = new Date().toTimeString().slice(0,8);
      const shortUrl = targetUrl.replace('https://api.upstox.com','').slice(0,60);
      console.log(`[${ts}] ${upstoxRes.status} ${shortUrl} | budget:${_rl.budget30m()}/30m`);

    }catch(e){
      const isTimeout = e.name === 'TimeoutError' || e.name === 'AbortError';
      const isNetDown = e.code === 'ECONNREFUSED' || e.code === 'ENOTFOUND' || e.code === 'ETIMEDOUT';
      // Return 503 for network-down so browser can distinguish from API error
      const status = isNetDown ? 503 : isTimeout ? 504 : 502;
      console.error(`[${new Date().toTimeString().slice(0,8)}] ERROR ${status}: ${e.message}`);
      res.writeHead(status, {...CORS_HEADERS, 'Content-Type': 'application/json'});
      res.end(JSON.stringify({error: e.message, status: 'error', netDown: isNetDown}));
    }
    return;
  }

  // ── Static file serving ──
  if(req.method === 'GET'){
    let filePath = path.join(__dirname, pathname === '/' ? '/index.html' : pathname);
    if(!filePath.startsWith(__dirname)){
      res.writeHead(403); res.end('Forbidden'); return;
    }
    const ext  = path.extname(filePath).toLowerCase();
    const mime = MIME[ext] || 'text/plain';
    fs.readFile(filePath, (err, data) => {
      if(err){
        if(err.code === 'ENOENT'){
          res.writeHead(404, {'Content-Type': 'text/plain'});
          res.end(`File not found: ${pathname}`);
        } else {
          res.writeHead(500); res.end('Internal error');
        }
        return;
      }
      res.writeHead(200, {...CORS_HEADERS, 'Content-Type': mime});
      res.end(data);
    });
    return;
  }

  res.writeHead(404, CORS_HEADERS);
  res.end('Not found');
}

// ═══════════════════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════════════════
const server = http.createServer(handler);

// Extended keep-alive timeouts for persistent proxy connections
server.keepAliveTimeout = 75000;
server.headersTimeout   = 76000;

server.listen(PORT, HOST, () => {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║      FreeOO AI PRO v1.0  — Local Proxy Server           ║');
  console.log('║  Upstox Plus: 5-WS Pool · full_d30 · No Restrictions    ║');
  console.log('╠══════════════════════════════════════════════════════════╣');
  console.log(`║  Server:    http://${HOST}:${PORT}                             ║`);
  console.log(`║  Dashboard: http://${HOST}:${PORT}/index.html                  ║`);
  console.log('║                                                          ║');
  console.log('║  All Upstox API calls run here — browser = display only ║');
  console.log('║  5-WS POOL (Upstox Plus):                               ║');
  console.log('║    WS1 INDEX_LTPC    ltpc         ≤6 keys  → 5000 limit ║');
  console.log('║    WS2 OPT_CURR      opt_greeks   ≤400 keys → 3000 limit║');
  console.log('║    WS3 OPT_NEXT      opt_greeks   ≤400 keys → 3000 limit║');
  console.log('║    WS4 FULL_D30_PLUS full_d30     ≤50 keys  → Plus only ║');
  console.log('║    WS5 OPT_EXTRA     opt_greeks   ≤400 keys → 3000 limit║');
  console.log('║                                                          ║');
  console.log('║  v1.0: OC Delay Fix · Chart Flicker Fix · Rate Guard    ║');
  console.log('║  v1.0+: SCROLL BLANK FIX — contain:paint replaces       ║');
  console.log('║    will-change on cards; GPU memory pressure eliminated  ║');
  console.log('║  v1.0+: TAB SWITCH FIX — rAF defer in switchOC/ocs10Switch║');
  console.log('║  v1.0+: ZERO-HANG OCS10 — yield+rAF innerHTML swap      ║');
  console.log('║  v1.0+: MODAL GPU FIX — animation + will-change:opacity  ║');
  console.log('║  v1.0+: LINK SPEED FIX — hover:hover guard + tap events  ║');
  console.log('║  Connectivity: 10s ping · 25min auth TTL · pagehide fix ║');
  console.log('║  Mobile: WS_AUTH_MIN_INTERVAL=300ms · Supervisor=4s      ║');
  console.log('║  Mobile: GPU layers · requestIdleCallback OCS rebuild    ║');
  console.log('║  Endpoints:                                              ║');
  console.log('║    GET /api/health              — health + WS pool info  ║');
  console.log('║    GET /api/ws-auth?connIdx=0-4 — WS auth (5 slots)     ║');
  console.log('║    GET /api/upstox              — REST API proxy         ║');
  console.log('║    GET /*                       — static file serving    ║');
  console.log('╠══════════════════════════════════════════════════════════╣');
  console.log('║  Stop server: Ctrl+C                                     ║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log('');
});

server.on('error', (e) => {
  if(e.code === 'EADDRINUSE'){
    console.error(`\nERROR: Port ${PORT} is already in use.`);
    console.error(`Try:  PORT=3001 node server.js\n`);
  } else {
    console.error('Server error:', e);
  }
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', () => { server.close(() => process.exit(0)); });
process.on('SIGINT',  () => { console.log('\nServer stopped.'); server.close(() => process.exit(0)); });
