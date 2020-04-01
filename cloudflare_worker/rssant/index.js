/**
 * POST /rss-proxy
 *
 * Parameters
 *  - token
 *  - url
 *  - headers
 *
 * Response
 *  - status
 *  - statusText
 *  - url
 *  - headers
 *  - body
 *  - error
 */
addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

function isNil(x) {
  return x === null || x === undefined
}

async function handleRequest(request) {
  let requestURL = new URL(request.url)
  if (request.method !== 'POST' || requestURL.pathname !== '/rss-proxy') {
    return new Response('404 Not Found', {
      status: 404,
      headers: {
        'content-type': 'text/html;charset=UTF-8',
      },
    })
  }
  const params = await request.json()
  const token = params['token']
  // TOKEN is global environment variable:
  // https://developers.cloudflare.com/workers/reference/apis/environment-variables/
  if (isNil(token) || token !== TOKEN) {
    return new Response('Who are you?', {
      status: 403,
      headers: {
        'content-type': 'text/html;charset=UTF-8',
      },
    })
  }
  const url = params['url']
  const headers = params['headers'] || {}
  if (isNil(url)) {
    return new Response('url is required', {
      status: 400,
      headers: {
        'content-type': 'text/html;charset=UTF-8',
      },
    })
  }
  let proxy_response = null
  let proxy_body = null
  let result = { url: url }
  let error = null
  try {
    proxy_response = await fetch(url, {
      method: 'GET',
      headers: headers,
    })
    proxy_body = await proxy_response.text()
  } catch (e) {
    error = e
  }
  if (!isNil(proxy_response)) {
    result.status = proxy_response.status
    result.statusText = proxy_response.statusText
    result.headers = proxy_response.headers
  }
  if (!isNil(proxy_body)) {
    result.body = proxy_body
  }
  if (!isNil(error)) {
    result.error = error.toString()
  }
  const init = {
    headers: {
      'content-type': 'application/json;charset=UTF-8',
    },
  }
  return new Response(JSON.stringify(result), init)
}
