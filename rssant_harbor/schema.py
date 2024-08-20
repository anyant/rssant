from validr import T

from rssant_api.models import FeedStatus
from rssant_common.validator import compiler
from rssant_feedlib.fulltext import FulltextAcceptStrategy

StorySchemaFields = dict(
    unique_id=T.str,
    title=T.str,
    content_hash_base64=T.str,
    author=T.str.optional,
    link=T.str.optional,
    image_url=T.url.optional,
    iframe_url=T.url.optional,
    audio_url=T.url.optional,
    has_mathjax=T.bool.optional,
    dt_published=T.datetime.object.optional.invalid_to_default,
    dt_updated=T.datetime.object.optional,
    summary=T.str.optional,
    content=T.str.optional,
    sentence_count=T.int.min(0).optional,
)

StoryOutputSchemaFields = StorySchemaFields.copy()
StoryOutputSchemaFields.update(
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
)

FeedSchemaFields = dict(
    url=T.url,
    use_proxy=T.bool.default(False),
    title=T.str,
    content_length=T.int.optional,
    content_hash_base64=T.str,
    link=T.str.optional,
    author=T.str.optional,
    icon=T.str.optional,
    description=T.str.optional,
    version=T.str.optional,
    dt_updated=T.datetime.object.optional,
    encoding=T.str.optional,
    etag=T.str.optional,
    last_modified=T.str.optional,
    response_status=T.int.optional,
    checksum_data_base64=T.str.maxlen(8192).optional,
    warnings=T.str.optional,
)

FeedOutputSchemaFields = FeedSchemaFields.copy()
FeedOutputSchemaFields.update(
    dt_updated=T.datetime.optional,
)

StorySchema = T.dict(**StorySchemaFields)
FeedSchema = T.dict(
    **FeedSchemaFields,
    storys=T.list(StorySchema),
)

FeedInfoSchemaFieldNames = [
    'response_status',
    'warnings',
]
FeedInfoSchemaFields = {k: FeedSchemaFields[k] for k in FeedInfoSchemaFieldNames}
FeedInfoSchema = T.dict(
    **FeedInfoSchemaFields,
    status=T.str.default(FeedStatus.READY),
)

StoryOutputSchema = T.dict(**StoryOutputSchemaFields)
FeedOutputSchema = T.dict(
    **FeedOutputSchemaFields,
    storys=T.list(StoryOutputSchema),
)

validate_feed_output = compiler.compile(FeedOutputSchema)

T_ACCEPT = T.enum(','.join(FulltextAcceptStrategy.__members__))
