interface Env {
  ASSETS: Fetcher
  MMWX_ORIGIN: string
  PROBE_TOKEN: string
  PROBE_HUB: DurableObjectNamespace
}

const PROBE_CACHE_TTL_SECONDS = 3
const HUB_SNAPSHOT_MAX_AGE_MS = 12_000
const HUB_IDLE_CLOSE_MS = 30_000
const HUB_RECONNECT_MAX_MS = 30_000
const HUB_NAME = 'global'
const HUB_CLIENT_TAG = 'probe-client'

type CloudflareCacheStorage = CacheStorage & { default: Cache }

function edgeCache(): Cache {
  return (caches as CloudflareCacheStorage).default
}

const routes: Record<string, string> = {
  '/api/probe': '/api/public/probe-servers',
  '/api/series': '/api/public/probe-series',
  '/api/stream': '/api/public/probe-ws',
}

function originURL(env: Env, pathname: string, search = ''): URL {
  const origin = new URL(env.MMWX_ORIGIN)
  if (origin.protocol !== 'https:' && origin.hostname !== '127.0.0.1' && origin.hostname !== 'localhost') {
    throw new Error('MMWX_ORIGIN must use HTTPS')
  }
  origin.pathname = pathname
  origin.search = search
  return origin
}

function upstreamURL(request: Request, env: Env): URL | null {
  const incoming = new URL(request.url)
  const path = routes[incoming.pathname]
  return path ? originURL(env, path, incoming.search) : null
}

function upstreamHeaders(request: Request, env: Env): Headers {
  const headers = new Headers(request.headers)
  headers.delete('cookie')
  headers.delete('authorization')
  headers.set('X-Forwarded-Host', new URL(request.url).host)
  headers.set('X-MMwx-Probe-Token', env.PROBE_TOKEN)
  return headers
}

function probeCacheKey(request: Request): Request | null {
  const incoming = new URL(request.url)
  if (incoming.pathname !== '/api/probe') return null
  // /api/probe 没有查询参数语义。统一 cache key，避免随机查询串绕过微缓存。
  incoming.search = ''
  return new Request(incoming.toString(), { method: 'GET' })
}

function clientResponse(
  response: Response,
  cacheStatus: 'HIT' | 'MISS' | 'BYPASS',
  source?: 'hub' | 'origin-fallback' | 'origin',
): Response {
  const headers = new Headers(response.headers)
  // Cache API 的副本可共享 3 秒；浏览器端仍不落盘，避免显示陈旧状态。
  headers.set('Cache-Control', 'private, no-store')
  headers.set('X-Probe-Cache', cacheStatus)
  if (source) headers.set('X-Probe-Source', source)
  headers.set('X-Content-Type-Options', 'nosniff')
  headers.delete('set-cookie')
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
}

function sanitizedResponse(response: Response): Response {
  const headers = new Headers(response.headers)
  headers.set('X-Content-Type-Options', 'nosniff')
  headers.delete('set-cookie')
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
}

function hubStub(env: Env): DurableObjectStub {
  return env.PROBE_HUB.getByName(HUB_NAME)
}

async function directUpstream(request: Request, env: Env, target: URL): Promise<Response> {
  return fetch(new Request(target, {
    method: 'GET',
    headers: upstreamHeaders(request, env),
  }))
}

/**
 * 全局 ProbeHub：无论 Worker 有多少访问域名或边缘节点，固定名称都映射到同一个
 * Durable Object。所有浏览器共享它到主控的一条 WebSocket。
 */
export class ProbeHub implements DurableObject {
  private readonly state: DurableObjectState
  private readonly env: Env
  private upstream: WebSocket | null = null
  private connecting: Promise<void> | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private idleTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectAttempts = 0
  private latestPayload: string | null = null
  private latestAt = 0
  private snapshotRequest: Promise<string> | null = null

  constructor(state: DurableObjectState, env: Env) {
    this.state = state
    this.env = env
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/stream') return this.openClient(request)
    if (url.pathname === '/snapshot') return this.snapshot()
    return new Response('Not found', { status: 404 })
  }

  webSocketClose(ws: WebSocket): void {
    try {
      ws.close(1000, 'client disconnected')
    } catch {
      // Socket may already be fully closed.
    }
    this.onClientCountChanged()
  }

  webSocketError(ws: WebSocket): void {
    try {
      ws.close(1011, 'client websocket error')
    } catch {
      // Socket may already be fully closed.
    }
    this.onClientCountChanged()
  }

  private openClient(request: Request): Response {
    if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
      return new Response('Expected WebSocket upgrade', { status: 426 })
    }

    const pair = new WebSocketPair()
    const [client, server] = Object.values(pair)
    this.state.acceptWebSocket(server, [HUB_CLIENT_TAG])
    this.cancelIdleClose()

    if (this.latestPayload && Date.now() - this.latestAt <= HUB_SNAPSHOT_MAX_AGE_MS) {
      server.send(this.latestPayload)
    } else {
      this.state.waitUntil(this.seedClientsFromSnapshot())
    }
    this.state.waitUntil(this.ensureUpstream())

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: { 'X-Probe-Hub': 'shared' },
    })
  }

  private async snapshot(): Promise<Response> {
    try {
      const payload = await this.ensureSnapshot()
      return new Response(payload, {
        headers: {
          'Cache-Control': 'private, no-store',
          'Content-Type': 'application/json; charset=utf-8',
          'X-Probe-Source': 'hub',
        },
      })
    } catch (error) {
      console.error('ProbeHub snapshot failed', error)
      return new Response('ProbeHub snapshot unavailable', { status: 502 })
    }
  }

  private clients(): WebSocket[] {
    return this.state.getWebSockets(HUB_CLIENT_TAG).filter((socket) => socket.readyState === WebSocket.OPEN)
  }

  private async seedClientsFromSnapshot(): Promise<void> {
    try {
      await this.ensureSnapshot()
    } catch (error) {
      // The upstream WebSocket can still deliver the first frame.
      console.warn('ProbeHub initial snapshot failed', error)
    }
  }

  private async ensureSnapshot(): Promise<string> {
    if (this.latestPayload && Date.now() - this.latestAt <= HUB_SNAPSHOT_MAX_AGE_MS) {
      return this.latestPayload
    }
    if (this.snapshotRequest) return this.snapshotRequest

    this.snapshotRequest = this.fetchSnapshot()
    try {
      return await this.snapshotRequest
    } finally {
      this.snapshotRequest = null
    }
  }

  private async fetchSnapshot(): Promise<string> {
    const response = await fetch(originURL(this.env, '/api/public/probe-servers'), {
      headers: { 'X-MMwx-Probe-Token': this.env.PROBE_TOKEN },
    })
    if (!response.ok) throw new Error(`upstream snapshot returned ${response.status}`)
    const payload = await response.text()
    if (!payload) throw new Error('upstream snapshot was empty')
    this.rememberAndBroadcast(payload)
    return payload
  }

  private async ensureUpstream(): Promise<void> {
    if (!this.clients().length) return
    if (this.upstream && (this.upstream.readyState === WebSocket.OPEN || this.upstream.readyState === WebSocket.CONNECTING)) {
      return
    }
    if (this.connecting) return this.connecting

    this.connecting = this.connectUpstream()
    try {
      await this.connecting
    } catch (error) {
      console.error('ProbeHub upstream connection failed', error)
      this.scheduleReconnect()
    } finally {
      this.connecting = null
    }
  }

  private async connectUpstream(): Promise<void> {
    const response = await fetch(originURL(this.env, '/api/public/probe-ws'), {
      headers: {
        Upgrade: 'websocket',
        'X-MMwx-Probe-Token': this.env.PROBE_TOKEN,
      },
    })
    const socket = response.webSocket
    if (response.status !== 101 || !socket) {
      throw new Error(`upstream websocket returned ${response.status}`)
    }

    socket.accept()
    this.upstream = socket
    this.reconnectAttempts = 0
    this.cancelReconnect()

    socket.addEventListener('message', (event) => {
      if (this.upstream !== socket) return
      const payload = typeof event.data === 'string'
        ? event.data
        : new TextDecoder().decode(event.data)
      if (payload) this.rememberAndBroadcast(payload)
    })
    socket.addEventListener('close', () => this.onUpstreamClosed(socket))
    socket.addEventListener('error', () => this.onUpstreamClosed(socket))
  }

  private rememberAndBroadcast(payload: string): void {
    this.latestPayload = payload
    this.latestAt = Date.now()
    for (const client of this.clients()) {
      try {
        client.send(payload)
      } catch {
        try {
          client.close(1011, 'broadcast failed')
        } catch {
          // Client is already gone.
        }
      }
    }
  }

  private onUpstreamClosed(socket: WebSocket): void {
    if (this.upstream !== socket) return
    this.upstream = null
    if (this.clients().length) this.scheduleReconnect()
  }

  private onClientCountChanged(): void {
    if (this.clients().length) {
      this.cancelIdleClose()
      this.state.waitUntil(this.ensureUpstream())
    } else {
      this.scheduleIdleClose()
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer || !this.clients().length) return
    const delay = Math.min(1_000 * (2 ** this.reconnectAttempts), HUB_RECONNECT_MAX_MS)
    this.reconnectAttempts += 1
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.state.waitUntil(this.ensureUpstream())
    }, delay)
  }

  private cancelReconnect(): void {
    if (!this.reconnectTimer) return
    clearTimeout(this.reconnectTimer)
    this.reconnectTimer = null
  }

  private scheduleIdleClose(): void {
    if (this.idleTimer) return
    this.cancelReconnect()
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null
      if (this.clients().length) return
      const socket = this.upstream
      this.upstream = null
      this.reconnectAttempts = 0
      if (socket) {
        try {
          socket.close(1000, 'ProbeHub idle')
        } catch {
          // Upstream already closed.
        }
      }
    }, HUB_IDLE_CLOSE_MS)
  }

  private cancelIdleClose(): void {
    if (!this.idleTimer) return
    clearTimeout(this.idleTimer)
    this.idleTimer = null
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const incoming = new URL(request.url)
    if (incoming.pathname === '/login') {
      return Response.redirect(new URL('/login', env.MMWX_ORIGIN).toString(), 302)
    }

    // 访客信息（Ran 主题访客浮卡用）——直接读 CF 请求头，不调用第三方
    if (incoming.pathname === '/api/visitor') {
      if (request.method !== 'GET') return new Response('Method not allowed', { status: 405 })
      const cf = request.cf
      const optionalNumber = (value: unknown): number | undefined => {
        if (typeof value !== 'string' && typeof value !== 'number') return undefined
        const parsed = Number(value)
        return Number.isFinite(parsed) ? parsed : undefined
      }
      return Response.json(
        {
          ip: request.headers.get('CF-Connecting-IP') || 'UNKNOWN',
          city: cf?.city,
          region: cf?.region,
          country: cf?.country,
          isp: cf?.asOrganization,
          lat: optionalNumber(cf?.latitude),
          lon: optionalNumber(cf?.longitude),
          risk: null,
          proxy: 'unknown',
          type: '',
        },
        {
          headers: {
            'Cache-Control': 'private, no-store',
            'Content-Type': 'application/json; charset=utf-8',
            'X-Content-Type-Options': 'nosniff',
          },
        },
      )
    }

    const target = upstreamURL(request, env)
    if (!target) return env.ASSETS.fetch(request)
    if (request.method !== 'GET') return new Response('Method not allowed', { status: 405 })
    if (!env.PROBE_TOKEN) {
      return new Response('Probe access secret is not configured', { status: 503 })
    }

    const cacheKey = probeCacheKey(request)
    if (cacheKey) {
      const cached = await edgeCache().match(cacheKey)
      if (cached) return clientResponse(cached, 'HIT')
    }

    if (incoming.pathname === '/api/stream') {
      try {
        const hubResponse = await hubStub(env).fetch(new Request('https://probe-hub.internal/stream', {
          method: 'GET',
          headers: request.headers,
        }))
        if (hubResponse.status === 101 && hubResponse.webSocket) return hubResponse
        console.warn(`ProbeHub stream returned ${hubResponse.status}; using direct upstream`)
      } catch (error) {
        console.error('ProbeHub stream unavailable; using direct upstream', error)
      }
      return directUpstream(request, env, target)
    }

    let upstream: Response
    let source: 'hub' | 'origin-fallback' | 'origin' = 'origin'
    if (cacheKey) {
      try {
        upstream = await hubStub(env).fetch('https://probe-hub.internal/snapshot')
        if (!upstream.ok) throw new Error(`ProbeHub snapshot returned ${upstream.status}`)
        source = 'hub'
      } catch (error) {
        console.error('ProbeHub snapshot unavailable; using direct upstream', error)
        upstream = await directUpstream(request, env, target)
        source = 'origin-fallback'
      }
    } else {
      upstream = await directUpstream(request, env, target)
    }

    // WebSocket 的 101 Response 必须原样返回，不能重新构造 body/headers。
    if (upstream.status === 101 || upstream.webSocket) return upstream
    const response = sanitizedResponse(upstream)

    if (cacheKey && upstream.ok) {
      const cacheCopy = response.clone()
      const cacheHeaders = new Headers(cacheCopy.headers)
      cacheHeaders.set('Cache-Control', `public, max-age=${PROBE_CACHE_TTL_SECONDS}`)
      cacheHeaders.set('X-Probe-Source', source)
      ctx.waitUntil(edgeCache().put(cacheKey, new Response(cacheCopy.body, {
        status: cacheCopy.status,
        statusText: cacheCopy.statusText,
        headers: cacheHeaders,
      })))
      return clientResponse(response, 'MISS', source)
    }

    return clientResponse(response, 'BYPASS', source)
  },
} satisfies ExportedHandler<Env>
