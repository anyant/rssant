/**
 * POST /rss-proxy
 *
 * Parameters
 *  - token
 *  - method
 *  - url
 *  - body
 *  - headers
 *
 * Response
 *  x-rss-proxy-status
 *  body stream
 *
 *
 * GET /request.get
 *
 * Parameters
 *   - token
 *   - url
 *
 * Response
 *   Origional response
 */
addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

function isBlank(x) {
  return x === null || x === undefined || x === ''
}

function errorResponse(status, message) {
  return new Response(message, {
    status: status,
    headers: {
      'content-type': 'text/plain;charset=utf-8',
    },
  })
}

function getEnvToken() {
  // TOKEN is global environment variable:
  // https://developers.cloudflare.com/workers/configuration/environment-variables/
  if (typeof TOKEN === 'undefined') {
    return 'rss-proxy'
  }
  return TOKEN
}

function isValidToeken(token) {
  return !isBlank(token) && token === getEnvToken()
}

async function handleRequest(request) {
  let requestURL = new URL(request.url)
  if (request.method === 'POST' && requestURL.pathname === '/rss-proxy') {
    return await handleRssProxy(request)
  }
  if (request.method === 'GET' && requestURL.pathname === '/request.get') {
    return await handleRequestGet(request)
  }
  return errorResponse(404, '404 Not Found')
}

PROXY_REQUEST_HEADERS = [
  'user-agent',
  'accept',
  'accept-encoding',
  'accept-language',
  'etag',
  'if-modified-since',
  'cache-control',
  'pragma',
]

PROXY_RESPONSE_HEADERS = [
  'content-encoding',
  'content-type',
  'cache-control',
  'etag',
  'last-modified',
  'expires',
  'age',
  'pragma',
  'server',
  'date',
]

async function handleRequestGet(request) {
  let requestURL = new URL(request.url)
  let url = requestURL.searchParams.get('url')
  if (isBlank(url)) {
    return errorResponse(400, 'url is required')
  }
  let token = requestURL.searchParams.get('token')
  if (!isValidToeken(token)) {
    return errorResponse(403, 'invalid token')
  }
  let requestHeaders = new Headers()
  for (let name of PROXY_REQUEST_HEADERS) {
    let value = request.headers.get(name)
    if (!isBlank(value)) {
      requestHeaders.append(name, value)
    }
  }
  let proxyResponse = await fetch(url, {
    method: 'GET',
    headers: requestHeaders,
    redirect: 'follow',
  })
  let proxyBody = proxyResponse.body
  let responseHeaders = new Headers()
  for (let name of PROXY_RESPONSE_HEADERS) {
    let value = proxyResponse.headers.get(name)
    if (!isBlank(value)) {
      responseHeaders.append(name, value)
    }
  }
  return new Response(proxyBody, {
    status: proxyResponse.status,
    headers: responseHeaders,
  })
}

async function handleRssProxy(request) {
  let params = null
  const contentType = request.headers.get('content-type')
  if (isBlank(contentType) || !contentType.includes('application/json')) {
    return errorResponse(400, 'content-type: application/json is required')
  }
  try {
    params = await request.json()
  } catch (e) {
    return errorResponse(400, 'invalid request body')
  }
  const token = params['token']
  if (!isValidToeken(token)) {
    return errorResponse(403, 'invalid token')
  }
  const url = params['url']
  if (isBlank(url)) {
    return errorResponse(400, 'url is required')
  }
  const method = params['method'] || 'GET'
  const body = params['body']
  const headers = params['headers'] || {}
  let proxy_response = null
  let proxy_body = null
  let proxy_headers = new Headers()
  let proxy_error = null
  try {
    proxy_response = await fetch(url, {
      method: method,
      headers: headers,
      body: body,
      redirect: 'follow',
    })
    proxy_body = proxy_response.body
  } catch (e) {
    proxy_error = e
    proxy_body = e.stack || e
  }
  if (!isBlank(proxy_response)) {
    for (var pair of proxy_response.headers.entries()) {
      proxy_headers.append(pair[0], pair[1])
    }
    proxy_headers.append('x-rss-proxy-status', proxy_response.status)
  }
  if (!isBlank(proxy_error)) {
    proxy_headers.append('x-rss-proxy-status', 'ERROR')
  }
  return new Response(proxy_body, { headers: proxy_headers })
}
