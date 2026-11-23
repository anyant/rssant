from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from django_rest_validr import RestRouter, T
from rssant_api.models.discover import (
    DISCOVER_EDITOR_INTRO_MAX_LENGTH,
    DiscoverError,
    DiscoverService,
)

DiscoverView = RestRouter()
DiscoverAdminView = RestRouter(permission_classes=[IsAdminUser])

DiscoverCategorySchema = T.dict(
    slug=T.str,
    name=T.str,
)

# 公共数据全站共用一份缓存，条目不带用户维度的 feed_id；前端用 url 和订阅列表比对。
DiscoverItemSchema = T.dict(
    category=T.str,
    url=T.str,
    title=T.str.optional,
    link=T.str.optional,
    editor_intro=T.str.optional,
    dt_latest_story_published=T.datetime.object.optional,
    # freeze_level 是系统检查频率倍率，受订阅人数和最近阅读影响；dryness 才是按发文量算的更新频率。
    freeze_level=T.int.min(0).optional,
    dryness=T.int.min(0).max(1000).optional,
)

DiscoverDataSchema = T.dict(
    categories=T.list(DiscoverCategorySchema),
    items=T.list(DiscoverItemSchema),
)

AdminCategorySchema = T.dict(
    slug=T.str,
    name=T.str,
    rank=T.int,
    deleted_at=T.datetime.object.optional,
)

AdminItemSchema = T.dict(
    category=T.str,
    url=T.str,
    title=T.str.optional,
    status=T.str,
    dt_checked=T.datetime.object.optional,
    dt_latest_story_published=T.datetime.object.optional,
    freeze_level=T.int.min(0).optional,
    dryness=T.int.min(0).max(1000).optional,
    editor_intro=T.str.optional,
    rank=T.int,
    deleted_at=T.datetime.object.optional,
)


def _bad_request(message):
    return Response({'message': message}, status=400)


@DiscoverView.post('discover.get')
def discover_get(request) -> DiscoverDataSchema:
    """发现页数据，一次返回分类和源"""
    return DiscoverService.get_public_data()


@DiscoverAdminView.post('discover.admin_categories')
def discover_admin_categories(
    request,
) -> T.dict(categories=T.list(AdminCategorySchema)):
    categories = DiscoverService.get_categories()
    return dict(categories=[category.to_dict() for category in categories])


@DiscoverAdminView.post('discover.admin_category_upsert')
def discover_admin_category_upsert(
    request,
    slug: T.str.maxlen(64).strip,
    name: T.str.maxlen(128).strip.optional,
    rank: T.int.min(0).optional,
) -> T.dict(category=AdminCategorySchema):
    try:
        category = DiscoverService.upsert_category(slug=slug, name=name, rank=rank)
    except DiscoverError as ex:
        return _bad_request(str(ex))
    return dict(category=category.to_dict())


@DiscoverAdminView.post('discover.admin_category_delete')
def discover_admin_category_delete(
    request,
    slug: T.str.maxlen(64).strip,
) -> T.dict(category=AdminCategorySchema):
    try:
        category = DiscoverService.delete_category(slug=slug)
    except DiscoverError as ex:
        return _bad_request(str(ex))
    return dict(category=category.to_dict())


@DiscoverAdminView.post('discover.admin_list')
def discover_admin_list(
    request,
    category: T.str.maxlen(64).strip.optional,
    include_deleted: T.bool.default(False),
    page: T.int.min(1).default(1),
) -> T.dict(
    total=T.int.min(0),
    page=T.int.min(1),
    items=T.list(AdminItemSchema),
):
    try:
        total, items = DiscoverService.list_items(
            category_slug=category,
            include_deleted=include_deleted,
            page=page,
        )
    except DiscoverError as ex:
        return _bad_request(str(ex))
    return dict(
        total=total,
        page=page,
        items=[item.to_dict() for item in items],
    )


@DiscoverAdminView.post('discover.admin_upsert')
def discover_admin_upsert(
    request,
    url: T.str.maxlen(4096).strip,
    category: T.str.maxlen(64).strip,
    editor_intro: T.str.maxlen(DISCOVER_EDITOR_INTRO_MAX_LENGTH).strip.optional,
    rank: T.int.min(0).optional,
) -> T.dict(item=AdminItemSchema):
    try:
        item = DiscoverService.upsert_item(
            url=url,
            category_slug=category,
            editor_intro=editor_intro,
            rank=rank,
        )
    except DiscoverError as ex:
        return _bad_request(str(ex))
    return dict(item=item.to_dict())


@DiscoverAdminView.post('discover.admin_remove')
def discover_admin_remove(
    request,
    url: T.str.maxlen(4096).strip,
    category: T.str.maxlen(64).strip,
) -> T.dict(item=AdminItemSchema):
    try:
        item = DiscoverService.remove_item(url=url, category_slug=category)
    except DiscoverError as ex:
        return _bad_request(str(ex))
    return dict(item=item.to_dict())
