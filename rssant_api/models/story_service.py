import logging
import typing

from django.utils import timezone
from django.forms.models import model_to_dict
from django.db import transaction
from validr import asdict

from rssant_common.detail import Detail
from rssant_config import CONFIG
from .seaweed_client import SeaweedClient
from .seaweed_story import SeaweedStory, SeaweedStoryStorage
from .story import Story, StoryDetailSchema
from .feed import Feed
from .feed_story_stat import FeedStoryStat
from .story_unique_ids import StoryUniqueIdsData


class CommonStory(SeaweedStory):

    def to_dict(self):
        return asdict(self)


LOG = logging.getLogger(__name__)


class StoryService:
    def __init__(self, storage: SeaweedStoryStorage):
        self._storage = storage

    def _to_common(self, story: Story):
        d = model_to_dict(story)
        content = d.get('content', None)
        if content:
            d['content_length'] = len(content)
        d['feed_id'] = story.feed_id
        return CommonStory(d)

    def _is_include_content(self, detail):
        detail = Detail.from_schema(detail, StoryDetailSchema)
        return 'content' in detail.include_fields

    def get_by_offset(self, feed_id, offset, detail=False) -> CommonStory:
        include_content = self._is_include_content(detail)
        seaweed_story = self._storage.get_story(feed_id, offset, include_content=include_content)
        if seaweed_story:
            return CommonStory(seaweed_story)
        try:
            story = Story.get_by_offset(feed_id, offset, detail=detail)
        except Story.DoesNotExist:
            return None
        return self._to_common(story)

    def seaweed_batch_get_by_offset(self, story_keys, detail=False) -> typing.List[CommonStory]:
        include_content = self._is_include_content(detail)
        seaweed_storys = self._storage.batch_get_story(
            story_keys, include_content=include_content)
        return [CommonStory(x) for x in seaweed_storys]

    def set_user_marked(self, feed_id, offset, is_user_marked=True):
        story = Story.get_by_offset(feed_id, offset)
        if (not story) and is_user_marked:
            common_story = self.get_by_offset(feed_id, offset, detail=True)
            d = asdict(common_story)
            d.pop('content_length', None)
            story = Story(**d)
            story.is_user_marked = is_user_marked
            story.save()
        elif story:
            Story.set_user_marked_by_id(story.id, is_user_marked=is_user_marked)

    def update_story(self, feed_id, offset, data: dict):
        old_story = self.get_by_offset(feed_id, offset, detail=True)
        for k, v in data.items():
            if v is not None:
                setattr(old_story, k, v)
        seaweed_story = SeaweedStory(old_story)
        self._storage.save_story(seaweed_story)

    def _get_unique_ids_by_stat(self, feed_id):
        stat = FeedStoryStat.objects\
            .only('unique_ids_data')\
            .filter(pk=feed_id).first()
        if not stat:
            return None
        result = {}
        unique_ids_data = StoryUniqueIdsData.decode(stat.unique_ids_data)
        for i, unique_id in enumerate(unique_ids_data.unique_ids):
            offset = unique_ids_data.begin_offset + i
            result[unique_id] = offset
        return result

    def _get_unique_ids_by_story(self, feed_id, begin_offset, end_offset):
        story_s = Story.objects\
            .only('offset', 'unique_id')\
            .filter(feed_id=feed_id)\
            .filter(offset__gte=begin_offset, offset__lt=end_offset)\
            .all()
        result = {}
        for story in story_s:
            result[story.unique_id] = story.offset
        return result

    def bulk_save_by_feed(self, feed_id, storys, batch_size=100, is_refresh=False):
        if not storys:
            return []  # story_objects
        storys = Story._dedup_sort_storys(storys)

        feed = Feed.get_by_pk(feed_id)
        offset = feed.total_storys

        unique_ids_map = self._get_unique_ids_by_stat(feed_id)
        if unique_ids_map is None:
            unique_ids_map = self._get_unique_ids_by_story(
                feed_id, offset - 100, offset)

        new_storys = []
        modified_storys = []
        now = timezone.now()

        for data in storys:
            unique_id = data['unique_id']
            is_story_exist = unique_id in unique_ids_map
            story = dict(data)
            story['feed_id'] = feed_id

            if is_story_exist:
                old_story = self.get_by_offset(
                    feed_id, offset, detail='+dt_published,content_hash_base64')
                if not old_story:
                    msg = 'story feed_id=%s unique_id=%r not consitent with unique_ids_data'
                    LOG.error(msg, feed_id, unique_id)
                    is_story_exist = False

            if is_story_exist:
                # 判断内容是否更新
                content_hash_base64 = story['content_hash_base64']
                is_modified = content_hash_base64 and content_hash_base64 != old_story.content_hash_base64
                if (not is_refresh) and (not is_modified):
                    continue
                # 发布时间只第一次赋值，不更新
                if old_story and old_story.dt_published:
                    story['dt_published'] = old_story.dt_published
                story['offset'] = unique_ids_map[unique_id]
            else:
                story['offset'] = offset

            story['dt_synced'] = now
            story = CommonStory(story)
            if is_story_exist:
                modified_storys.append(story)
            else:
                new_storys.append(story)
                offset += 1

        new_total_storys = offset
        return_storys = modified_storys + new_storys

        for story in modified_storys + new_storys:
            seaweed_story = SeaweedStory(story)
            self._storage.save_story(seaweed_story)

        tmp_unique_ids = {y: x for x, y in unique_ids_map.items()}
        for story in new_storys:
            tmp_unique_ids[story.offset] = story.unique_id
        new_unique_ids = []
        for offset in reversed(range(max(0, new_total_storys - 100), new_total_storys)):
            unique_id = tmp_unique_ids.get(offset, '')
            if not unique_id:
                msg = 'wrong unique_ids_data, feed_id=%s offset=%s: %r'
                LOG.error(msg, feed_id, offset, tmp_unique_ids)
                break
            new_unique_ids.append(unique_id)
            new_begin_offset = offset
        unique_ids_data = StoryUniqueIdsData(
            new_begin_offset, new_unique_ids).encode()

        if new_storys:
            with transaction.atomic():
                FeedStoryStat.save_unique_ids_data(feed_id, unique_ids_data)
                Story._update_feed_monthly_story_count(feed, new_storys)
                feed.total_storys = new_total_storys
                if feed.dt_first_story_published is None:
                    feed.dt_first_story_published = new_storys[0].dt_published
                feed.dt_latest_story_published = new_storys[-1].dt_published
                feed.save()

        return return_storys

    def _delete_seaweed_by_retention(self, feed_id, begin_offset, end_offset):
        for offset in range(begin_offset, end_offset, 1):
            self._storage.delete_story(feed_id, offset)

    def delete_by_retention(self, feed_id, retention=3000, limit=1000):
        """
        Params:
            feed_id: feed ID
            retention: num storys to keep
            limit: delete at most limit rows
        """
        with transaction.atomic():
            feed = Feed.get_by_pk(feed_id)
            offset = feed.retention_offset or 0
            # delete at most limit rows, avoid out of memory and timeout
            new_offset = min(offset + limit, feed.total_storys - retention)
            if new_offset > offset:
                self._delete_seaweed_by_retention(feed_id, offset, new_offset)
                n = Story.delete_by_retention_offset(feed_id, new_offset)
                feed.retention_offset = new_offset
                feed.save()
                return n
        return 0


SEAWEED_CLIENT = SeaweedClient(
    CONFIG.seaweed_volume_url,
    thread_pool_size=CONFIG.seaweed_thread_pool_size,
)
STORY_SERVICE = StoryService(SeaweedStoryStorage(SEAWEED_CLIENT))
