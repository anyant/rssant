import logging
import time
import datetime

from django.utils import timezone
from django.db import transaction
from validr import T, asdict, modelclass, fields

from rssant_common.detail import Detail
from rssant_common.validator import compiler
from rssant_config import CONFIG
from .story import Story, StoryDetailSchema
from .story_info import StoryInfo, StoryId
from .feed import Feed
from .feed_story_stat import FeedStoryStat
from .story_unique_ids import StoryUniqueIdsData
from .story_storage import PostgresClient, PostgresStoryStorage


LOG = logging.getLogger(__name__)


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

    def __repr__(self):
        base = f'{type(self).__name__}#{self.feed_id},{self.offset}'
        return f'<{base} unique_id={self.unique_id!r} title={self.title!r}>'


class StoryService:
    def __init__(self, storage: PostgresStoryStorage):
        self._storage = storage

    @staticmethod
    def to_common(story: Story) -> CommonStory:
        d = {}
        for key in fields(CommonStory):
            value = story.__dict__.get(key, None)
            if value is not None:
                d[key] = value
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
        story_info = StoryInfo.get(feed_id, offset, detail=detail)
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

    def set_user_marked(self, feed_id, offset, is_user_marked=True) -> Story:
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
        return story

    def _validate_update_story_params(self, data: dict):
        story = CommonStory(
            feed_id=0,
            offset=0,
            unique_id='#',
            title='#',
        )
        for k, v in data.items():
            setattr(story, k, v)
        story = story.to_dict()
        update_params = {}
        for k in data.keys():
            if k in story:
                update_params[k] = story[k]
        return update_params

    def update_story(self, feed_id, offset, data: dict):
        data = {k: v for k, v in data.items() if v is not None}
        keys = set(data.keys())
        if keys == {'content'}:
            self._storage.save_content(feed_id, offset, data['content'])
            return
        # assume StoryInfo already created when bulk_save_by_feed
        update_params = self._validate_update_story_params(data)
        content = update_params.pop('content', None)
        if content:
            self._storage.save_content(feed_id, offset, content)
        story_id = StoryId.encode(feed_id, offset)
        with transaction.atomic():
            updated = StoryInfo.objects\
                .filter(pk=story_id)\
                .update(**update_params)
            if updated <= 0:
                msg = 'story#%s,%s not found in StoryInfo when update story'
                LOG.warning(msg, feed_id, offset)

    def _get_unique_ids_by_stat(self, feed_id):
        stat = FeedStoryStat.objects\
            .only('unique_ids_data')\
            .filter(pk=feed_id).seal().first()
        if not stat or not stat.unique_ids_data:
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
            .seal().all()
        result = {}
        for story in story_s:
            unique_id = story.unique_id.replace('\n').strip()
            result[unique_id] = story.offset
        return result

    def _get_unique_ids(self, feed_id, feed_total_story):
        unique_ids_map = self._get_unique_ids_by_stat(feed_id)
        if unique_ids_map is None:
            begin_offset = max(0, feed_total_story - 300)
            unique_ids_map = self._get_unique_ids_by_story(
                feed_id, begin_offset, feed_total_story)
        return unique_ids_map

    def _group_storys(self, storys, unique_ids_map):
        old_storys_map = {}
        new_storys = []
        for story in storys:
            offset = unique_ids_map.get(story['unique_id'])
            if offset is None:
                new_storys.append(story)
            else:
                old_storys_map[offset] = story
        return old_storys_map, new_storys

    def _query_story_infos(self, feed_id, offset_s):
        keys = [(feed_id, offset) for offset in offset_s]
        detail = '+dt_published,dt_created,content_hash_base64'
        story_infos = StoryInfo.batch_get(keys, detail=detail)
        return story_infos

    def _query_story_objects(self, feed_id, offset_s):
        keys = [(feed_id, offset) for offset in offset_s]
        detail = '+dt_published,dt_created,content_hash_base64'
        storys = Story.batch_get_by_offset(keys, detail=detail)
        return storys

    def _query_old_story_objects(self, feed_id, old_storys_map):
        old_story_infos = self._query_story_infos(feed_id, old_storys_map.keys())
        remain_offsets = set(old_storys_map.keys()) - {x.offset for x in old_story_infos}
        old_story_objects = self._query_story_objects(feed_id, remain_offsets)
        old_story_objects_map = {}
        for story_info in old_story_infos:
            old_story_objects_map[story_info.offset] = (True, story_info)
        for story_object in old_story_objects:
            old_story_objects_map[story_object.offset] = (False, story_object)
        return old_story_objects_map

    def _compute_modified_storys(self, feed_id, old_storys_map, new_storys, is_refresh):
        old_story_objects_map = self._query_old_story_objects(feed_id, old_storys_map)
        modified_story_objects = {}
        new_storys = list(new_storys)
        for offset, story in old_storys_map.items():
            if offset not in old_story_objects_map:
                msg = 'story feed_id=%s offset=%r not consitent with unique_ids_data'
                LOG.error(msg, feed_id, offset)
                new_storys.append(story)
            else:
                is_story_info, old_story = old_story_objects_map[offset]
                new_hash = story['content_hash_base64']
                if not new_hash:
                    is_modified = True
                else:
                    is_modified = old_story.content_hash_base64 != new_hash
                if is_refresh or is_modified:
                    modified_story_objects[offset] = (is_story_info, old_story, story)
        return new_storys, modified_story_objects

    def _common_story_of(self, story, feed_id, offset, now):
        story['feed_id'] = feed_id
        story['offset'] = offset
        story['dt_synced'] = now
        for key in ['dt_created', 'dt_updated']:
            if not story.get(key):
                story[key] = now
        if story.get('content'):
            story['content_length'] = len(story['content'])
        story = CommonStory(story)
        return story

    def _compute_storys_map(self, feed_id, feed_total_story, new_storys, modified_story_objects):
        storys_map = {}
        now = timezone.now()
        for offset, story in enumerate(new_storys, feed_total_story):
            story = self._common_story_of(story, feed_id, offset, now)
            storys_map[offset] = (story, None, False)
        for offset, (is_story_info, old_story, story) in modified_story_objects.items():
            story = self._common_story_of(story, feed_id, offset, now)
            # 发布时间只第一次赋值，不更新
            if old_story.dt_published:
                story.dt_published = old_story.dt_published
            # 创建时间只第一次赋值，不更新
            if old_story.dt_created:
                story.dt_created = old_story.dt_created
            storys_map[offset] = (story, old_story, is_story_info)
        return storys_map

    def _compute_story_infos(self, storys_map):
        new_story_infos = []
        modified_story_infos = []
        for offset, (story, old_story, is_story_info) in storys_map.items():
            if (not old_story) or (not is_story_info):
                story_id = StoryId.encode(story.feed_id, story.offset)
                story_info = StoryInfo(id=story_id)
                new_story_infos.append(story_info)
            else:
                assert isinstance(old_story, StoryInfo)
                story_info = old_story
                modified_story_infos.append(old_story)
            update_params = story.to_dict()
            update_params.pop('content', None)
            update_params.pop('feed_id', None)
            update_params.pop('offset', None)
            for k, v in update_params.items():
                setattr(story_info, k, v)
        new_story_infos = list(sorted(new_story_infos, key=lambda x: x.offset))
        modified_story_infos = list(sorted(modified_story_infos, key=lambda x: x.offset))
        return new_story_infos, modified_story_infos

    def _compute_new_unique_ids_data(self, feed_id, new_total_storys, modified_storys, unique_ids_map) -> bytes:
        # 容忍旧的 unique_ids_map 有错，用新的正确的值覆盖旧值
        tmp_unique_ids = dict(unique_ids_map)
        for story in modified_storys:
            tmp_unique_ids[story.unique_id] = story.offset
        tmp_unique_ids = {y: x for x, y in tmp_unique_ids.items()}
        new_unique_ids = []
        size = min(len(tmp_unique_ids), 300)
        begin_offset = max(0, new_total_storys - size)
        new_begin_offset = new_total_storys
        for offset in reversed(range(begin_offset, new_total_storys)):
            unique_id = tmp_unique_ids.get(offset, None)
            if not unique_id:
                msg = 'wrong unique_ids_data, feed_id=%s offset=%s: %r'
                LOG.error(msg, feed_id, offset, tmp_unique_ids)
                break
            new_unique_ids.insert(0, unique_id)
            new_begin_offset = offset
        if len(new_unique_ids) != len(set(new_unique_ids)):
            msg = 'found feed_id=%s begin_offset=%s duplicate new_unique_ids, will discard it: %r'
            LOG.error(msg, feed_id, new_begin_offset, new_unique_ids)
            return None
        unique_ids_data = StoryUniqueIdsData(
            new_begin_offset, new_unique_ids).encode()
        return unique_ids_data

    def bulk_save_by_feed(self, feed_id, storys, batch_size=100, is_refresh=False):
        if not storys:
            return []  # modified_common_storys
        storys = Story._dedup_sort_storys(storys)

        feed = Feed.get_by_pk(feed_id)
        unique_ids_map = self._get_unique_ids(feed_id, feed.total_storys)

        old_storys_map, new_storys = self._group_storys(storys, unique_ids_map)

        new_storys, modified_story_objects = self._compute_modified_storys(
            feed_id, old_storys_map, new_storys, is_refresh=is_refresh,
        )
        new_storys = Story._dedup_sort_storys(new_storys)
        new_total_storys = feed.total_storys + len(new_storys)

        storys_map = self._compute_storys_map(
            feed_id, feed.total_storys, new_storys, modified_story_objects,
        )

        new_common_storys = []
        modified_common_storys = []
        save_story_contents = []
        for offset, (story, old_story, is_story_info) in storys_map.items():
            modified_common_storys.append(story)
            if not old_story:
                new_common_storys.append(story)
            save_story_contents.append(((feed_id, offset), story.content))

        new_story_infos, modified_story_infos = self._compute_story_infos(storys_map)

        unique_ids_data = self._compute_new_unique_ids_data(
            feed_id, new_total_storys, modified_common_storys, unique_ids_map,
        )

        save_content_begin = time.time()
        self._storage.batch_save_content(save_story_contents)
        save_content_cost = int((time.time() - save_content_begin) * 1000)
        LOG.info('storage.save_content %d cost=%dms', len(save_story_contents), save_content_cost)

        with transaction.atomic():
            for story_info in modified_story_infos:
                story_info.save()
            if new_story_infos:
                StoryInfo.objects.bulk_create(new_story_infos, batch_size=batch_size)
            if new_common_storys:
                FeedStoryStat.save_unique_ids_data(feed_id, unique_ids_data)
                Story._update_feed_monthly_story_count(feed, new_common_storys)
                feed.total_storys = new_total_storys
                if feed.dt_first_story_published is None:
                    feed.dt_first_story_published = new_common_storys[0].dt_published
                feed.dt_latest_story_published = new_common_storys[-1].dt_published
                feed.save()

        return modified_common_storys

    def _delete_content_by_retention(self, feed_id, begin_offset, end_offset):
        keys = []
        for offset in range(begin_offset, end_offset, 1):
            keys.append((feed_id, offset))
        self._storage.batch_delete_content(keys)

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
        self._delete_content_by_retention(feed_id, offset, new_offset)
        with transaction.atomic():
            n = StoryInfo.delete_by_retention_offset(feed_id, new_offset)
            m = Story.delete_by_retention_offset(feed_id, new_offset)
            feed.retention_offset = new_offset
            feed.save()
            return n + m
        return 0


POSTGRES_CLIENT = PostgresClient(CONFIG.pg_story_volumes_parsed)
POSTGRES_STORY_STORAGE = PostgresStoryStorage(POSTGRES_CLIENT)

STORY_SERVICE = StoryService(POSTGRES_STORY_STORAGE)
