// ═══════════════════════════════════════════════════════════════════════
// FreeOO AI PRO v1.0 — Service Worker (freeoo-sw.js) · Upstox API v2+v3
// ═══════════════════════════════════════════════════════════════════════
//
// PURPOSE:
//   Intercepts browser's  GET /api/upstox?url=...&token=...  requests.
//   The Service Worker runs outside the page — it has NO CORS restriction.
//   So it calls Upstox v2/v3 directly with the Authorization header.
//   The browser page (index.html) only handles display.
//
// API VERSION MAPPING:
//   v2: /market-quote/quotes, /option/chain
//   v3: /market-quote/ltp, /market-quote/ohlc, /historical-candle/*,
//       /feed/market-data-feed/authorize, /market-information/exchange-status
//
// DEPLOYMENT (GitHub Pages):
//   1. Put both files in your repo root:
//        index.html
//        freeoo-sw.js              ← this file
//   2. Push to GitHub Pages.
//   3. Open the HTML — SW auto-registers and handles all Upstox v3 API calls.
//   4. No server, no CORS proxy, no node.js needed.
//
// HOW IT WORKS:
//   Browser  ──GET /api/upstox?url=...&token=...──▶  Service Worker
//   SW       ──GET https://api.upstox.com/v3/...──▶  Upstox v3 (no CORS!)
//   SW       ◀── raw JSON ────────────────────────── Upstox v3
//   Browser  ◀── raw JSON ─────────────────────────  SW
//
// NOTE:
//   Service Workers only work on HTTPS or localhost.
//   For local file:// use server.js instead.
//
// UPSTOX PLUS MEMBER — 5 WebSocket Connections:
//   This app is optimised for Upstox Plus (5 WS connections allowed).
//   5-WS Architecture:
//     WS1 (INDEX_LTPC)    — LTPC for 6 index instruments. Dedicated = 5000 key limit.
//     WS2 (OPT_CURR)      — option_greeks for current expiry ATM±25 strikes.
//     WS3 (OPT_NEXT)      — option_greeks for next expiry ATM±25 strikes.
//     WS4 (FULL_D30_PLUS) — full_d30 for 50 key instruments (Plus-exclusive mode).
//     WS5 (OPT_EXTRA)     — option_greeks for additional instruments / BankNifty.
//   Each connection reconnects independently — if one drops, others keep flowing.
//   No rate-limit risk: each WS is well within its per-connection subscription limit.
//   No close-code 4002 risk: 5 connections used (Plus limit = 5).
//
// v1.0 CHANGES (current):
//   - Version updated to v1.0 across all 3 files (index.html · server.js · freeoo-sw.js)
//   - [RETAINED] WS stale handler guard: onclose/onerror/onmessage/ping only act on current WS
//   - [RETAINED] WS subscriptions sent as binary frames (Upstox v3 docs requirement)
//   - [FIX] Chart historical↔intraday flicker eliminated
//   - [FIX] Option chain 30s delay fixed — true tick-by-tick in ALL expiries
//   - [FIX] Rate limit safety — 5s REST floor when WS not live
//   - All prior v1411 fixes retained (5-WS pool · keepalive · retry-after · 503 hint · etc.)
//
// v1.0+ IMPROVEMENTS (this build):
//   - FIRST-OPEN FIX: Busy-lock force-release after env detect — Chain/Quote data loads
//     instantly via server/SW instead of waiting for slow CORS-proxy timeout
//   - MOBILE SCROLL FIX: GPU-layer CSS (translateZ) on all cards — no blank sections on scroll
//   - RECONNECT FIX: WS_AUTH_MIN_INTERVAL 2000ms→300ms — all 5 slots re-auth in <1.5s
//   - PING FIX: WS_PING_INTERVAL 15s→10s — mobile radios kill idle TCP after ~12s
//   - HEARTBEAT FIX: WS heartbeat 2s→1.5s — detects dead connections 25% faster
//   - SUPERVISOR FIX: Interval 5s→4s, stuck timer 9s→6s — faster dead-slot recovery
//   - OC REBUILD FIX: Mobile debounce 3s→5s + requestIdleCallback — zero scroll hang
//   - ENV DETECT FIX: Health timeout 600ms→400ms, SW wait 1500ms→800ms
//   - OPTION SUB FIX: Refresh interval 5s→3s + immediate resub on chain load
//   - SW API timeout: 6s (was 8s) — matches server.js for consistency
//
// CONNECTIVITY FIXES:
//   - SW_API_TIMEOUT_MS: 6s (was 8s — faster timeout catches dead connections sooner)
//   - WS auth: 3 retries (was 2) + 100ms/300ms progressive retry delays (same as before)
//   - TICK message: responds with ACK so page can detect SW liveness
//   - SW keep-alive: periodic event listener on a MessageChannel
//   - Feature list updated to reflect all connectivity improvements
//   - Health check response updated with new feature list
// ═══════════════════════════════════════════════════════════════════════

const SW_VERSION = 'freeoo-v1.0-sw'; // v1.0: 5-WS Pool · Plus Member · full_d30 mode

// ── API timeout — 6s for REST calls (was 8s — 6s catches dead connections faster while
//    still generous enough for slow mobile 4G and Upstox API latency spikes) ──
const SW_API_TIMEOUT_MS = 6000;

// ── WS Auth timeout — 10s (auth involves a redirect on slow connections) ──
const SW_WS_AUTH_TIMEOUT_MS = 10000;

// ── Install: skip waiting so new SW activates immediately ──
self.addEventListener('install', event => {
  console.log(`[FreeOO SW ${SW_VERSION}] Installing…`);
  self.skipWaiting();
});

// ── Activate: claim all clients immediately ──
self.addEventListener('activate', event => {
  console.log(`[FreeOO SW ${SW_VERSION}] Activated — claiming all clients`);
  event.waitUntil(self.clients.claim());
});

// ── Fetch: intercept /api/upstox, /api/health, /api/ws-auth requests ──
self.addEventListener('fetch', event => {
  const reqUrl = new URL(event.request.url);

  // ── Health check ──
  if(reqUrl.pathname === '/api/health'){
    event.respondWith(new Response(
      JSON.stringify({
        status: 'ok',
        server: 'FreeOO-v1.0-SW-v3API',
        wsInfo: '5-WS Pool · Plus member · WS0=INDEX_LTPC · WS1=OPT_CURR · WS2=OPT_NEXT · WS3=FULL_D30_PLUS · WS4=OPT_EXTRA · 5 conn limit (Plus) · v1.0',
        time: new Date().toISOString(),
        version: SW_VERSION,
        features: [
          '5-ws-pool','plus-member','full-d30-mode',
          '6s-timeout','keepalive','retry-after-forwarding',
          '503-net-hint','ws-auth-retry-3x-100ms-300ms','option-greeks-mode',
          'market-info-first-tick','ws-independent-reconnect',
          'ws-pool-supervisor','zombie-ws-detection',
          'adaptive-stale-12s-8s-6s','ws-ping-10s',
          'adaptive-ws-bypass-3s-2s-1.2s','sub-delay-200ms-400ms',
          'pagehide-pageshow-handlers','freeze-resume-handlers',
          'ws-auth-ttl-25min','reconnect-sched-tracker',
          'heartbeat-1.5s','keepalive-10s','bgSync-12s',
          'first-open-busy-release','auth-min-300ms','supervisor-4s',
          'mobile-gpu-layers','idle-callback-ocs','mobile-oc-5s-debounce'
        ]
      }),
      {status:200, headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}}
    ));
    return;
  }

  // ── Upstox API proxy ──
  if(reqUrl.pathname === '/api/upstox'){
    event.respondWith(handleUpstoxRequest(reqUrl));
    return;
  }

  // ── WebSocket auth endpoint (v3) ──
  // Returns Upstox v3 WSS authorized URL.
  // Supports ?connIdx=0-4 — each of the 5 WS connections gets its own URL.
  // Upstox auth tokens are single-use, one-per-connection.
  if(reqUrl.pathname === '/api/ws-auth'){
    event.respondWith(handleWsAuth(reqUrl));
    return;
  }

  // All other requests: pass through normally to network
  event.respondWith(fetch(event.request));
});

// ═══════════════════════════════════════════════════════════════════════
// CORE HANDLER — calls Upstox from SW context (no CORS restriction)
// Forwards Retry-After from 429; returns 503 on network-down; 504 on timeout
// ═══════════════════════════════════════════════════════════════════════
async function handleUpstoxRequest(reqUrl){
  const CORS_HEADERS = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Cache-Control': 'no-store'
  };

  const targetUrl = reqUrl.searchParams.get('url');
  const token     = reqUrl.searchParams.get('token');

  if(!targetUrl || !token){
    return new Response(JSON.stringify({error:'Missing url or token'}),
      {status:400, headers:{...CORS_HEADERS,'Content-Type':'application/json'}});
  }

  // Security: only allow Upstox v2 + v3 API URLs
  const ALLOWED = ['https://api.upstox.com/v2/', 'https://api.upstox.com/v3/'];
  if(!ALLOWED.some(b => targetUrl.startsWith(b))){
    return new Response(JSON.stringify({error:'URL not allowed'}),
      {status:403, headers:{...CORS_HEADERS,'Content-Type':'application/json'}});
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), SW_API_TIMEOUT_MS);

  try{
    const upstoxRes = await fetch(targetUrl, {
      method:  'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Accept':        'application/json',
        'User-Agent':    'FreeOO-v1.0-SW/100'
      },
      keepalive: true,
      signal: controller.signal
    });

    clearTimeout(timeout);

    // Forward Retry-After header from 429 so page can honor the backoff window
    const extraHeaders = {};
    if(upstoxRes.status === 429){
      const ra = upstoxRes.headers.get('retry-after') || upstoxRes.headers.get('Retry-After');
      if(ra) extraHeaders['X-Retry-After'] = ra;
    }

    const body = await upstoxRes.arrayBuffer();

    return new Response(body, {
      status:  upstoxRes.status,
      headers: {
        ...CORS_HEADERS,
        ...extraHeaders,
        'Content-Type': upstoxRes.headers.get('content-type') || 'application/json'
      }
    });

  }catch(e){
    clearTimeout(timeout);
    const isTimeout = e.name === 'AbortError';
    const isNetDown = e.name === 'TypeError';
    // Return 503 for network-down (distinct from API errors) — page uses this to show offline badge
    const status = isTimeout ? 504 : isNetDown ? 503 : 502;
    return new Response(
      JSON.stringify({error: e.message, status:'error', timeout: isTimeout, netDown: isNetDown}),
      {
        status,
        headers: {...CORS_HEADERS, 'Content-Type':'application/json'}
      }
    );
  }
}

// ═══════════════════════════════════════════════════════════════════════
// WS AUTH HANDLER — fetches Upstox v3 authorized WSS URL (SW-side, no CORS)
// Supports ?connIdx=0-4 for 5-WS pool (each connection needs its own URL).
// 3 retries with faster progressive delays: immediate → 100ms → 300ms.
// Longer timeout 10s (was 8s) — auth endpoint can be slower on mobile.
// ═══════════════════════════════════════════════════════════════════════
async function handleWsAuth(reqUrl){
  const CORS_HEADERS = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Cache-Control': 'no-store'
  };
  const token = reqUrl.searchParams.get('token');
  const connIdx = reqUrl.searchParams.get('connIdx') || '0';

  if(!token){
    return new Response(JSON.stringify({error:'Missing token'}),
      {status:400, headers:{...CORS_HEADERS,'Content-Type':'application/json'}});
  }

  const WS_AUTH_URL = 'https://api.upstox.com/v3/feed/market-data-feed/authorize';

  // 3 attempts with faster progressive delays: immediate → 100ms → 300ms
  // First WS slot (INDEX_LTPC) must reconnect fast to restore LTP feed.
  const retryDelays = [0, 100, 300];
  for(let attempt = 0; attempt < retryDelays.length; attempt++){
    if(retryDelays[attempt] > 0){
      await new Promise(r => setTimeout(r, retryDelays[attempt]));
    }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), SW_WS_AUTH_TIMEOUT_MS);
    try{
      const upstoxRes = await fetch(WS_AUTH_URL, {
        method: 'GET',
        headers:{
          'Authorization': `Bearer ${token}`,
          'Accept':        'application/json',
          'User-Agent':    `FreeOO-v1.0-SW/100-WS${connIdx}`
        },
        keepalive: true,
        signal: controller.signal
      });
      clearTimeout(timeout);

      // Auth success (2xx) or a definitive auth failure (401/403) — don't retry these
      const body = await upstoxRes.arrayBuffer();
      return new Response(body, {
        status: upstoxRes.status,
        headers: {
          ...CORS_HEADERS,
          'Content-Type': upstoxRes.headers.get('content-type') || 'application/json',
          'X-WS-Auth-Attempt': String(attempt + 1)
        }
      });

    }catch(e){
      clearTimeout(timeout);
      if(attempt < retryDelays.length - 1){
        // Transient error — retry with delay
        console.warn(`[FreeOO SW v1.0] WS auth attempt ${attempt+1} failed (${e.message}) — retrying in ${retryDelays[attempt+1]}ms`);
        continue;
      }
      // All retries exhausted
      const isTimeout = e.name === 'AbortError';
      return new Response(
        JSON.stringify({error: e.message, timeout: isTimeout, attempt: attempt+1, connIdx}),
        {status: isTimeout ? 504 : 502, headers:{...CORS_HEADERS,'Content-Type':'application/json'}}
      );
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════
// MESSAGE HANDLER — allow page to check SW status + keep SW alive
// CONNECTIVITY FIX: TICK now responds with ACK so page can detect liveness.
//   The page sends TICK every 3s via anti-throttle loop.
//   This keeps the SW from being terminated during long market sessions.
// ═══════════════════════════════════════════════════════════════════════
self.addEventListener('message', event => {
  if(!event.data) return;

  // PING — page checking if SW is alive
  if(event.data.type === 'PING'){
    try{
      event.source.postMessage({
        type: 'PONG',
        version: SW_VERSION,
        ready: true,
        ts: Date.now(),
        features: [
          'retry-after','keepalive','503-net-hint','ws-auth-retry-3x',
          '5-ws-pool','plus-member','full_d30-mode',
          'option-greeks-mode','ws-pool-supervisor','ws-conn-idx-support',
          'zombie-ws-detection','adaptive-stale-12s-8s-6s',
          'adaptive-bypass-3s-2s-1.2s','sub-delay-200ms-400ms',
          'pagehide-pageshow','freeze-resume','ws-auth-ttl-25min',
          'heartbeat-1.5s','keepalive-10s',
          'first-open-busy-release','auth-min-300ms','supervisor-4s',
          'mobile-gpu-layers','idle-callback-ocs','mobile-oc-5s-debounce'
        ]
      });
    }catch(e){}
    return;
  }

  // SKIP_WAITING — force new SW to activate
  if(event.data.type === 'SKIP_WAITING'){
    self.skipWaiting();
    return;
  }

  // TICK — anti-throttle heartbeat from page (sent every 3s)
  // Respond with ACK so page knows SW is alive and processing events.
  // This keep-alive pattern prevents the browser from terminating
  // the SW during long inactive periods (common on mobile).
  if(event.data.type === 'TICK'){
    try{
      event.source.postMessage({
        type: 'TICK_ACK',
        version: SW_VERSION,
        ts: Date.now()
      });
    }catch(e){}
    return;
  }
});
