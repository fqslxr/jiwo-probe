interface Env {
  ASSETS: Fetcher
  MMWX_ORIGIN: string
  PROBE_TOKEN: string
}

const PROBE_CACHE_TTL_SECONDS = 3

type CloudflareCacheStorage = CacheStorage & { default: Cache }

function edgeCache(): Cache {
  return (caches as CloudflareCacheStorage).default
}

const routes: Record<string, string> = {
  '/api/probe': '/api/public/probe-servers',
  '/api/series': '/api/public/probe-series',
  '/api/stream': '/api/public/probe-ws',
}

function upstreamURL(request: Request, env: Env): URL | null {
  const incoming = new URL(request.url)
  const path = routes[incoming.pathname]
  if (!path) return null

  const origin = new URL(env.MMWX_ORIGIN)
  if (origin.protocol !== 'https:' && origin.hostname !== '127.0.0.1' && origin.hostname !== 'localhost') {
    throw new Error('MMWX_ORIGIN must use HTTPS')
  }
  origin.pathname = path
  origin.search = incoming.search
  return origin
}

function probeCacheKey(request: Request): Request | null {
  const incoming = new URL(request.url)
  if (incoming.pathname !== '/api/probe') return null
  // /api/probe 没有查询参数语义。统一 cache key，避免随机查询串绕过微缓存。
  incoming.search = ''
  return new Request(incoming.toString(), { method: 'GET' })
}

function clientResponse(response: Response, cacheStatus: 'HIT' | 'MISS' | 'BYPASS'): Response {
  const headers = new Headers(response.headers)
  // Cache API 的副本可共享 3 秒；浏览器端仍不落盘，避免显示陈旧状态。
  headers.set('Cache-Control', 'private, no-store')
  headers.set('X-Probe-Cache', cacheStatus)
  headers.set('X-Content-Type-Options', 'nosniff')
  headers.delete('set-cookie')
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
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

    const headers = new Headers(request.headers)
    headers.delete('cookie')
    headers.delete('authorization')
    headers.set('X-Forwarded-Host', new URL(request.url).host)
    headers.set('X-MMwx-Probe-Token', env.PROBE_TOKEN)

    const upstream = await fetch(new Request(target, { method: 'GET', headers }))
    // WebSocket 的 101 Response 必须原样返回，不能重新构造 body/headers。
    if (upstream.status === 101 || upstream.webSocket) return upstream

    const responseHeaders = new Headers(upstream.headers)
    responseHeaders.set('X-Content-Type-Options', 'nosniff')
    responseHeaders.delete('set-cookie')
    const response = new Response(upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: responseHeaders,
    })

    if (cacheKey && upstream.ok) {
      const cacheCopy = response.clone()
      const cacheHeaders = new Headers(cacheCopy.headers)
      cacheHeaders.set('Cache-Control', `public, max-age=${PROBE_CACHE_TTL_SECONDS}`)
      ctx.waitUntil(edgeCache().put(cacheKey, new Response(cacheCopy.body, {
        status: cacheCopy.status,
        statusText: cacheCopy.statusText,
        headers: cacheHeaders,
      })))
      return clientResponse(response, 'MISS')
    }

    return clientResponse(response, 'BYPASS')
  },
} satisfies ExportedHandler<Env>
