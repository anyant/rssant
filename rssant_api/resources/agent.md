# 蚁阅接口说明

蚁阅（RSSAnt）的 HTTP 接口，按日常阅读旅程组织。所有接口需要令牌。

## 鉴权

请求头：Authorization: Token <令牌>
Token 是鉴权方式的名字，后面填你的令牌。
令牌在 蚁阅首页右上角菜单 →「用 AI 阅读」里复制。

## Base URL

{origin}/api/v1

## 每日精选（推荐技能）

每天读订阅、写一份 Markdown 精选的默认流程：{origin}/docs/daily-digest.md
想用别的读法，可以按自己的习惯调接口，不必用这份技能。

## 读订阅

### 列出订阅
POST /feed.query
Body: {"detail": false}
返回 feeds[]：id（unionid）、title、url、group、num_unread_storys、story_offset、total_storys。
订阅多时返回很大（几百 KB），不要直接打印；需要订阅标题时优先用 story 接口返回的 feed.title。

### 单个订阅
POST /feed.get
Body: {"id": "<unionid>", "detail": false}

## 发现订阅源

### 发现页
POST /discover.get
Body: {}
返回 categories[]（slug、name）和 items[]：category、url、title、link、editor_intro、dt_latest_story_published、freeze_level、dryness。
dryness 是更新频率：<500 资讯、500-750 周更、>750 月更；数值越小更新越频繁。
只读。用 url 和 feed.query 的订阅列表比对，判断是否已订阅。

## 添加订阅

### 按网址添加
POST /feed.import
Body: {"text": "https://example.com/feed.xml", "group": "可选分组"}
text 可以是单个网址、多个网址、OPML/XML 或含链接的文本。
返回 total、num_created_feeds、num_existed_feeds、num_feed_creations、created_feeds[]、feed_creations[]。
feed_creations 表示还在抓取解析的订阅，用 feed.get_creation 查进度；成功后出现在 feed.query。

### 导入文件
POST /feed.import_file
multipart/form-data：file=OPML/书签文件；group 放在 query 参数。

### 查询抓取进度
POST /feed.get_creation
Body: {"id": 123, "detail": false}
返回 is_ready、status、feed_id、message；detail=true 时失败原因在 error_message（仅 status=error 时非空）。status=ready 且 feed_id 有值时订阅完成。

### 查询最近添加
POST /feed.query_creation
Body: {"limit": 100, "detail": false}
返回 feed_creations[]，字段同上。

## 管理订阅

### 改标题
POST /feed.set_title
Body: {"id": "<unionid>", "title": "新标题"}
title 省略 = 恢复原标题。

### 改分组
POST /feed.set_group
Body: {"id": "<unionid>", "group": "新分组"}
group 省略 = 取消分组。

### 批量分组
POST /feed.set_all_group
Body: {"ids": ["<unionid>"], "group": "新分组"}
返回 num_updated。

### 删除订阅
POST /feed.delete
Body: {"id": "<unionid>"}

## 读文章

先取候选，再取正文。列表只给摘要；正文用批量接口或单篇取，不要逐篇抓网页。

### 最近新增
POST /story.query_recent
Body: {"days": 1, "unread_only": true, "limit": 100, "per_feed_limit": 3, "detail": false}
返回 storys[]：id、offset、feed（id、title）、title、link、dt_published、dt_created、is_read、is_favorited。
days 是最近多少天新收录（1-30，默认 14），按 dt_created 判断，不是发布时间。
unread_only=true 只返回未读（单篇未标已读，且 offset >= story_offset）。
limit 最大 200；per_feed_limit 默认 3，是每个订阅最多返回几篇。
按 dt_published 倒序；total 是时间窗内的篇数（每个订阅最多计 per_feed_limit 篇）。这是“今天有什么新内容”的入口；detail=true 只返回 summary，列表不带正文。

### 按订阅取文章
POST /story.query
Body: {"feed_id": "<unionid>", "offset": 0, "size": 20, "detail": false}
size 最大 100；offset 省略时从阅读位置开始；detail=true 返回 summary，不返回正文。

### 批量取正文
POST /story.query_batch
Body: {"storys": [{"feed_id": "<unionid>", "offset": 0, "limit": 1}], "detail": true, "content": true, "content_format": "markdown"}
limit 最大 10（每个订阅一段连续文章）；detail=true 返回 summary；
content=true 才从正文存储读取 content（默认 false）；content_format=html（默认）或 markdown。
一次取 10-15 篇，不要逐篇 story.get。

### 单篇
POST /story.get
Body: {"feed_id": "<unionid>", "offset": 0, "detail": true, "set_readed": false, "content_format": "html"}
offset 必填；set_readed=true 会推进阅读位置。

### 全文
POST /story.fetch_fulltext
Body: {"feed_id": "<unionid>", "offset": 0}
返回 response_status、accept、story；源站不允许时 accept=REJECT；单次最多 50 秒，按需调用，不要批量抓。

## 未读和已读

- is_read 返回读者视角的真实已读：单篇已读，或 offset < story_offset（阅读位置之前），任一即已读。不需要客户端再拿 offset 判断。
- unread_only=true 用同一口径过滤。
- num_unread_storys 是订阅的累计未读数（总数 - 阅读位置 - 单篇已读），可能很大；不是当天新增。当天新增看 story.query_recent。
- 标记：单篇 story.set_read；推进阅读位置 feed.set_offset；全部已读 feed.set_all_readed。改数据前先确认。
- 单篇取消已读（story.set_read is_read=false）只对阅读位置之后的文章有效；位置之前的仍算已读（阅读位置只增不减）。

## 标记

### 单篇已读
POST /story.set_read
Body: {"feed_id": "<unionid>", "offset": 0, "is_read": true}
is_read=false 取消已读；阅读位置之前的文章取消无效，仍算已读。

### 阅读位置
POST /feed.set_offset
Body: {"id": "<unionid>", "offset": 123}
offset 省略 = 全部已读；只增不减。

### 全部已读
POST /feed.set_all_readed
Body: {"ids": ["<unionid>"]}
省略 ids 表示全部订阅；返回 num_updated。

### 收藏
POST /story.set_favorited
Body: {"feed_id": "<unionid>", "offset": 0, "is_favorited": true}

### 关注/看过
POST /story.set_watched
Body: {"feed_id": "<unionid>", "offset": 0, "is_watched": true}

### 收藏列表
POST /story.query_favorited
Body: {"detail": false}

### 看过列表
POST /story.query_watched
Body: {"detail": false}

## 导出

GET /feed/export/opml
GET /story/export/favorited
GET /feed/<feed_id>/export/stories

导出 zip 结构：manifest.json、articles.json、index.xlsx、content/*.md；单订阅额外有 feed.json。

## 约定

- 时间是 UTC；展示转北京时间。
- size 最大 100；story.get 的 offset 必填；列表 detail=true 只返回摘要，不带正文。
- 正文：列表不带正文（前端列表页用 detail=true 取 10-20 篇，只需要摘要）；正文用 story.query_batch content=true 或 story.get。content 为空说明源只有摘要，用 summary 兜底，或用单篇 fetch_fulltext。
- 错误返回 {"message": "..."} 或 {"detail": "..."}。
- 401 表示令牌已失效。重置过就在 首页右上角菜单 →「用 AI 阅读」重新复制提示词；退出登录后需要先重新登录。
- 改数据（改标题/分组、删除、导入、添加订阅、标已读、收藏）前先向用户确认。
- 请求节奏：接口约 1 次/秒（突发 30 次），批量操作一次请求完成；遇到 503/429 等 1-2 秒重试，最多 2 次。
- 完整接口：{origin}/docs/v1/（需登录）。
