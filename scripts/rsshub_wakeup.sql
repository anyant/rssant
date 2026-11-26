-- RSSHub 存量错误源一次性唤醒（部署任务手工执行，不在应用启动时自动跑）
--
-- 背景：RSSHub 源平均腐烂，5,053 条 error 里很多被 freeze_level 压到很久才检查一次。
-- 这里把“RSSHub 源 + error + 至少一个订阅者”的 dt_checked 置空，调度器下一轮立即抓取；
-- 之后仍按原有冻结节奏重试，不改 freeze_level。
--
-- 用法：先跑第一步看命中量，再按 300 条一批执行第二步。
--   :last_id 初始 0，每批用返回的最大 id 更新，直到没有返回。
--   用 psql 变量传游标：
--     psql -v last_id=0 -f scripts/rsshub_wakeup.sql   # 第一步 + 第二批共用文件时按需拆分
--   或者只执行第二步片段：psql -v last_id=0 -c "$(sed -n '/WITH picked/,/RETURNING f.id;/p' scripts/rsshub_wakeup.sql)"
-- 主机清单和 rssant_feedlib/rsshub.py 的 _PUBLIC_HOSTS 一致，由单测校验，改清单要同步这里。

-- 第一步：只看命中量（只读）
SELECT count(*) AS total,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM rssant_api_userfeed uf WHERE uf.feed_id = f.id
       )) AS with_subscriber
FROM rssant_api_feed AS f
WHERE f.status = 'error'
  AND (
      lower(split_part(split_part(f.url, '//', 2), '/', 1)) LIKE '%rsshub%'
      OR lower(split_part(split_part(f.url, '//', 2), '/', 1)) IN (
          'holoxx.f5.si', 'hub.slarker.me', 'rss.4040940.xyz', 'rss.datuan.dev',
          'rss.fatpandac.com', 'rss.littlebaby.life', 'rss.owo.nz', 'rss.spriple.org',
          'rss.wudifeixue.com', 'rsshub-balancer.virworks.moe', 'rsshub-instance.zeabur.app',
          'rsshub-ljp.herokuapp.com', 'rsshub.220303.xyz', 'rsshub.aierliz.xyz', 'rsshub.app',
          'rsshub.atgw.io', 'rsshub.cups.moe', 'rsshub.email-once.com', 'rsshub.feeded.xyz',
          'rsshub.friesport.ac.cn', 'rsshub.gneko.io', 'rsshub.henry.wang', 'rsshub.ioiox.com',
          'rsshub.isrss.com', 'rsshub.ktachibana.party', 'rsshub.kukmoon.com',
          'rsshub.liumingye.cn', 'rsshub.moerats.com', 'rsshub.mubibai.com',
          'rsshub.mystery0.app', 'rsshub.pseudoyu.com', 'rsshub.raincandy.net',
          'rsshub.rss.tips', 'rsshub.rssforever.com', 'rsshub.shunyu.wang', 'rsshub.umzzz.com',
          'rsshub.uneasy.win', 'rsshub.woodland.cafe', 'rsshub.zsliang.me'
      )
  );

-- 第二步：分批唤醒（每批 300 条，观察 429 再跑下一批）
WITH picked AS (
    SELECT f.id
    FROM rssant_api_feed AS f
    WHERE f.status = 'error'
      AND f.id > :last_id
      AND (
          lower(split_part(split_part(f.url, '//', 2), '/', 1)) LIKE '%rsshub%'
          OR lower(split_part(split_part(f.url, '//', 2), '/', 1)) IN (
              'holoxx.f5.si', 'hub.slarker.me', 'rss.4040940.xyz', 'rss.datuan.dev',
              'rss.fatpandac.com', 'rss.littlebaby.life', 'rss.owo.nz', 'rss.spriple.org',
              'rss.wudifeixue.com', 'rsshub-balancer.virworks.moe',
              'rsshub-instance.zeabur.app', 'rsshub-ljp.herokuapp.com', 'rsshub.220303.xyz',
              'rsshub.aierliz.xyz', 'rsshub.app', 'rsshub.atgw.io', 'rsshub.cups.moe',
              'rsshub.email-once.com', 'rsshub.feeded.xyz', 'rsshub.friesport.ac.cn',
              'rsshub.gneko.io', 'rsshub.henry.wang', 'rsshub.ioiox.com', 'rsshub.isrss.com',
              'rsshub.ktachibana.party', 'rsshub.kukmoon.com', 'rsshub.liumingye.cn',
              'rsshub.moerats.com', 'rsshub.mubibai.com', 'rsshub.mystery0.app',
              'rsshub.pseudoyu.com', 'rsshub.raincandy.net', 'rsshub.rss.tips',
              'rsshub.rssforever.com', 'rsshub.shunyu.wang', 'rsshub.umzzz.com',
              'rsshub.uneasy.win', 'rsshub.woodland.cafe', 'rsshub.zsliang.me'
          )
      )
      AND EXISTS (SELECT 1 FROM rssant_api_userfeed uf WHERE uf.feed_id = f.id)
    ORDER BY f.id
    LIMIT 300
)
UPDATE rssant_api_feed AS f
SET dt_checked = NULL, _version = f._version + 1
FROM picked
WHERE f.id = picked.id
RETURNING f.id;
