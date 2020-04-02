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

async function handleRequest(request) {
  let requestURL = new URL(request.url)
  if (request.method !== 'POST' || requestURL.pathname !== '/rss-proxy') {
    return errorResponse(404, '404 Not Found')
  }
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
  // TOKEN is global environment variable:
  // https://developers.cloudflare.com/workers/reference/apis/environment-variables/
  if (isBlank(token) || token !== TOKEN) {
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
