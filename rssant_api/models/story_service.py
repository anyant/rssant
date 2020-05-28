import logging
import datetime

from django.utils import timezone
from django.forms.models import model_to_dict
from django.db import transaction
from validr import T, asdict, modelclass

from rssant_common.detail import Detail
from rssant_common.validator import compiler
from rssant_config import CONFIG
from .seaweed_client import SeaweedClient
from .seaweed_story import SeaweedStoryStorage
from .story import Story, StoryDetailSchema
from .story_info import StoryInfo, StoryId, STORY_INFO_DETAIL_FEILDS
from .feed import Feed
from .feed_story_stat import FeedStoryStat
from .story_unique_ids import StoryUniqueIdsData


@modelclass(compiler=compiler)
class CommonStory:
    feed_id: int = T.int
    offset: int = T.int
    unique_id: str = T.str
    title: str = T.str
    link: str = T.str.optional
    author: str = T.str.optional
    image_url: str = T.str.optional
    audio_url: str = T.str.optional
    iframe_url: str = T.str.optional
    has_mathjax: bool = T.bool.optional
    dt_published: datetime.datetime = T.datetime.object.optional
    dt_updated: datetime.datetime = T.datetime.object.optional
    dt_created: datetime.datetime = T.datetime.object.optional
    dt_synced: datetime.datetime = T.datetime.object.optional
    summary: str = T.str.optional
    content: str = T.str.optional
    content_length: int = T.int.min(0).optional
    content_hash_base64: str = T.str.optional

    def to_dict(self):
        return asdict(self)


LOG = logging.getLogger(__name__)


class StoryService:
    def __init__(self, storage: SeaweedStoryStorage):
        self._storage = storage

    @staticmethod
    def to_common(story: Story):
        d = model_to_dict(story)
        content = d.get('content', None)
        if content:
            d['content_length'] = len(content)
        d['dt_created'] = story.dt_created
        d['feed_id'] = story.feed_id
        d['offset'] = story.offset
        return CommonStory(d)

    def _is_include_content(self, detail):
        detail = Detail.from_schema(detail, StoryDetailSchema)
        return 'content' in detail.include_fields

    def get_by_offset(self, feed_id, offset, detail=False) -> CommonStory:
        q = StoryInfo.objects.filter(pk=StoryId.encode(feed_id, offset))
        detail = Detail.from_schema(detail, StoryDetailSchema)
        if not detail:
            q = q.defer(*STORY_INFO_DETAIL_FEILDS)
        story_info = q.first()
        if story_info:
            story = self.to_common(story_info)
            include_content = self._is_include_content(detail)
            if include_content:
                content = self._storage.get_content(feed_id, offset)
                story.content = content
            return story
        try:
            story = Story.get_by_offset(feed_id, offset, detail=detail)
        except Story.DoesNotExist:
            return None
        return self.to_common(story)

    def set_user_marked(self, feed_id, offset, is_user_marked=True):
        try:
            story = Story.get_by_offset(feed_id, offset)
        except Story.DoesNotExist:
            story = None
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
        story = self.get_by_offset(feed_id, offset, detail=True)
        for k, v in data.items():
            if v is not None:
                setattr(story, k, v)
        self._storage.save_content(feed_id, offset, story.content)
        update_params = story.to_dict()
        update_params.pop('content', None)
        update_params.pop('feed_id', None)
        update_params.pop('offset', None)
        story_id = StoryId.encode(feed_id, offset)
        updated = StoryInfo.objects\
            .filter(pk=story_id)\
            .update(**update_params)
        if updated <= 0:
            story_info = StoryInfo(pk=story_id, **update_params)
            story_info.save()

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

        bulk_update_objects = []
        bulk_create_objects = []
        for story in modified_storys:
            story_id = StoryId.encode(story.feed_id, story.offset)
            story_info = StoryInfo.objects.filter(pk=story_id).first()
            if not story_info:
                story_info = StoryInfo(id=story_id)
                bulk_create_objects.append(story_info)
            else:
                bulk_update_objects.append(story_info)
            update_params = story.to_dict()
            update_params.pop('content', None)
            update_params.pop('feed_id', None)
            update_params.pop('offset', None)
            for k, v in update_params.items():
                setattr(story_info, k, v)
        for story in new_storys:
            story_id = StoryId.encode(story.feed_id, story.offset)
            story_info = StoryInfo(id=story_id)
            bulk_create_objects.append(story_info)
            update_params = story.to_dict()
            update_params.pop('content', None)
            update_params.pop('feed_id', None)
            update_params.pop('offset', None)
            for k, v in update_params.items():
                setattr(story_info, k, v)

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

        for story in modified_storys + new_storys:
            self._storage.save_content(
                story.feed_id, story.offset, story.content)

        with transaction.atomic():
            for story_info in bulk_update_objects:
                story_info.save()
            if bulk_create_objects:
                StoryInfo.objects.bulk_create(bulk_create_objects, batch_size=batch_size)
            if new_storys:
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
            self._storage.delete_content(feed_id, offset)

    def delete_by_retention(self, feed_id, retention=3000, limit=1000):
        """
        Params:
            feed_id: feed ID
            retention: num storys to keep
            limit: delete at most limit rows
        """
        feed = Feed.get_by_pk(feed_id)
        offset = feed.retention_offset or 0
        # delete at most limit rows, avoid out of memory and timeout
        new_offset = min(offset + limit, feed.total_storys - retention)
        if new_offset <= offset:
            return 0
        self._delete_seaweed_by_retention(feed_id, offset, new_offset)
        with transaction.atomic():
            n = StoryInfo.delete_by_retention_offset(feed_id, new_offset)
            m = Story.delete_by_retention_offset(feed_id, new_offset)
            feed.retention_offset = new_offset
            feed.save()
            return max(m, n)
        return 0


SEAWEED_CLIENT = SeaweedClient(
    CONFIG.seaweed_volume_url,
    thread_pool_size=CONFIG.seaweed_thread_pool_size,
)
STORY_SERVICE = StoryService(SeaweedStoryStorage(SEAWEED_CLIENT))
