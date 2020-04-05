## 快速部署

Cloudflare Worker：https://workers.cloudflare.com/

1. 注册 Cloudflare 账号，免费创建 Worker。
2. 将 `index.js` 代码粘贴上去，点击发布。
3. 随机生成一个密钥，然后在【Environment Variables】页面上设置为 TOKEN。

<p>
<img src="./cloudflare-worker.png" width="80%" alt="cloudflare-worker" />
</p>

## 本地开发

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
