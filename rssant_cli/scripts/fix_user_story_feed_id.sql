--- 修复feed merge导致user story数据不一致的问题
WITH target AS (
    SELECT us.id AS id, us.feed_id AS old_feed_id, uf.feed_id  AS new_feed_id
    FROM rssant_api_userstory AS us
    JOIN rssant_api_userfeed AS uf ON us.user_feed_id = uf.id
    WHERE us.feed_id != uf.feed_id
)
UPDATE rssant_api_userstory AS us
SET feed_id=target.new_feed_id
FROM target WHERE us.id=target.id
RETURNING target.id, us.user_id, target.old_feed_id, target.new_feed_id;
