## Cloudflare Worker

https://developers.cloudflare.com/workers/tooling/wrangler/install/

```
npm i @cloudflare/wrangler -g

wrangler preview
wrangler build
wrangler publish
```

**Update Secret**:

```
openssl rand 16 -hex
wrangler secret put TOKEN
```
