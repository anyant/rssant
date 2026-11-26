# 蚁阅接口说明

蚁阅（RSSAnt）的 HTTP 接口，按日常阅读旅程组织。所有接口需要令牌。

## 鉴权

请求头：Authorization: Token <令牌>
Token 是鉴权方式的名字，后面填你的令牌。
令牌在 蚁阅首页右上角菜单 →「用 AI 阅读」里复制。

## Base URL

{origin}/api/v1

## 读订阅

### 列出订阅
POST /feed.query
Body: {"detail": false}
返回 feeds[]：id（unionid）、title、url、group、num_unread_storys、story_offset、total_storys。

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

### 按订阅取文章
POST /story.query
Body: {"feed_id": "<unionid>", "offset": 0, "size": 20, "detail": true}
size 最大 100；offset 省略时从阅读位置开始；detail=true 才返回 summary/content。

### 最近文章
POST /story.query_recent
Body: {"days": 7, "detail": false}
最多 100 篇；feed_ids 可选；只返回近 days 天有更新的文章，可能为空。

### 指定文章
POST /story.query_batch
Body: {"storys": [{"feed_id": "<unionid>", "offset": 0, "limit": 1}], "detail": true}
limit 最大 10。

### 单篇
POST /story.get
Body: {"feed_id": "<unionid>", "offset": 0, "detail": true, "set_readed": false}
offset 必填；set_readed=true 会推进阅读位置。

### 全文
POST /story.fetch_fulltext
Body: {"feed_id": "<unionid>", "offset": 0}
返回 response_status、accept、story；源站不允许时 accept=REJECT；单次最多 50 秒，按需调用，不要批量抓。

## 标记

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
- size 最大 100；story.get 的 offset 必填；detail=true 才有正文/摘要。
- 错误返回 {"message": "..."} 或 {"detail": "..."}。
- 401 表示令牌已失效。重置过就在 首页右上角菜单 →「用 AI 阅读」重新复制提示词；退出登录后需要先重新登录。
- 改数据（改标题/分组、删除、导入、添加订阅、标已读、收藏）前先向用户确认。
- 完整接口：{origin}/docs/v1/（需登录）。
