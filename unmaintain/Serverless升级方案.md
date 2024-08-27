定时任务
实时调用

api接口
harbor接口

worker接口

scheduler服务

接口互相调用问题

worker实例单独运行
任务缓存到数据库，分优先级，带有效期
sync_feed,find_feed,story_fetch_fulltext
sync_feed每次取100个，有效期24小时
find_feed每次取不限数量，有效期12小时
story_fetch_fulltext，有效期1小时
单个写入：key唯一upsert
批量写入：insert update
取出：update where, delete where
过期： delete where，定期执行

# 虚拟任务队列，提供取任务的API
从多个来源，按优先级取任务，取到之后返回给消费者（可以批量取，缓存起来）
api_task:find_feed/fetch_story （直接调用worker接口，优先级1）
retry_find_feed （取10条任务写入临时队列，优先级2）
check_feed/sync_feed （取10条任务写入临时队列，优先级3）
update_feed/sync_full_text/fetch_story（取10条任务写入临时队列，优先级4）

# 定时任务，提供API接口即可，被scheduler调用
harbor_rss.clean_by_retention
harbor_rss.clean_feedurlmap_by_retention
harbor_rss.feed_refresh_freeze_level
harbor_rss.feed_detect_and_merge_duplicate

# 入库任务，提供API接口即可，被worker调用
harbor_rss.update_feed_creation_status
harbor_rss.save_feed_creation_result
harbor_rss.update_feed_info
harbor_rss.update_story

# 入库任务，触发fetch_story，被worker调用
# 升级方案：去掉触发fetch_story，变成普通API接口。fetch_story由虚拟队列触发。
harbor_rss.update_feed

# 定时任务，触发sync_feed，被scheduler调用
# 升级方案：虚拟任务队列
harbor_rss.check_feed

# 定时任务，触发find_feed，被scheduler调用
# 升级方案：去掉触发find_feed，变成普通API接口。find_feed由虚拟队列触发。
harbor_rss.clean_feed_creation

# 在线服务，抓取全文内容
# 升级方案：直接调用worker接口
harbor_rss.sync_story_fulltext

# 爬虫任务，提供API接口，执行完通过harbor提交结果
worker_rss.find_feed
worker_rss.sync_feed
worker_rss.fetch_story
worker_rss.process_story_webpage
DNS_SERVICE.refresh 后台线程执行

# scheduler任务调度器，服务器上单实例部署
定时任务，调用harbor接口
调用harbor接口取任务，调用worker接口执行任务

db,img,api,worker,scheduler
