from validr import T

from rssant_common.validator import compiler


# https://github.com/kurtmckee/feedparser
# https://pythonhosted.org/feedparser/
Detailed = T.dict(
    type=T.str.optional.desc(""),
    base=T.str.optional.desc(""),
    language=T.str.optional.desc(""),
    value=T.str.optional.desc(""),
)

UserInfo = T.dict(
    name=T.str.optional.desc("The name of this user"),
    href=T.str.optional.desc("The URL of this user"),
    email=T.str.optional.desc("The email address of this user"),
)

CommonInfo = dict(
    link=T.str.optional.desc("The primary link of this feed/entry"),
    title=T.str.optional.desc("The title of the feed/entry"),
    id=T.str.optional.desc("A globally unique identifier"),
    title_detail=Detailed.optional.desc("The title of the feed/entry"),
    description=T.str.optional.desc("The description of the feed/entry"),
    published=T.str.optional.desc("The date the feed/entry was published"),
    published_parsed=T.datetime.object.optional.invalid_to_default.desc("The date the feed/entry was published"),
    updated=T.str.optional.desc("The date the feed/entry was updated"),
    updated_parsed=T.datetime.object.optional.invalid_to_default.desc("The date the feed/entry was updated"),
    author=T.str.optional.desc("The author of this feed/entry"),
    author_detail=UserInfo.optional.desc("Details about the feed/entry author"),
    tags=T.list(
        T.dict(
            term=T.str.optional.desc("The category term (keyword)"),
            scheme=T.str.optional.desc("The category scheme (domain)"),
            label=T.str.optional.desc("A human-readable label for the category"),
        )
    ).optional.desc("Details of the categories for the feed/entry"),
    license=T.str.optional.desc(
        "A URL of the license under which this entry is distributed"
    ),
)

StorySchema = T.dict(
    **CommonInfo,
    summary=T.str.optional.desc("A summary of the entry"),
    summary_detail=Detailed.optional.desc("A summary of the entry"),
    content=T.list(Detailed).optional.desc(
        "Details about the full content of the entry"
    ),
    contributors=T.list(UserInfo).optional.desc(
        "Contributors (secondary authors) to this entry"
    ),
    links=T.list(
        T.dict(
            rel=T.str.optional.desc(
                "The relationship of this entry link, "
                "standard rel values: alternate enclosure related self via"
            ),
            type=T.str.optional.desc("The content type of the linked page"),
            href=T.str.optional.desc("The URL of the linked page"),
            title=T.str.optional.desc("The title of this entry link"),
        )
    ).optional.desc("Details on the links associated with the feed"),
    enclosures=T.list(
        T.dict(
            type=T.str.optional.desc("The content type of the linked file"),
            length=T.int.optional.invalid_to_default.desc("The length of the linked file"),
            href=T.str.optional.desc("The URL of the linked file"),
        )
    ).optional.desc("A list of links to external files associated with this entry"),
    expired=T.str.optional.desc("The date this entry is set to expire"),
    expired_parsed=T.datetime.object.optional.invalid_to_default.desc("The date this entry is set to expire"),
    created=T.str.optional.desc("The date this entry was first created (drafted)"),
    created_parsed=T.datetime.object.optional.invalid_to_default.desc(
        "The date this entry was first created (drafted)"
    ),
)

FeedSchema = T.dict(
    **CommonInfo,
    language=T.str.optional.desc("The primary language of the feed"),
    subtitle=T.str.optional.desc(
        "A subtitle, tagline, slogan, or other short description of the feed"
    ),
    subtitle_detail=Detailed.optional.desc(
        "A subtitle, tagline, slogan, or other short description of the feed"
    ),
    icon=T.str.optional.desc("A URL to a small icon representing the feed"),
    logo=T.str.optional.desc("A URL to a graphic representing a logo for the feed"),
    info=T.str.optional.desc(
        "Free-form human-readable description of the feed format itself"
    ),
    info_detail=Detailed.optional.desc(
        "Free-form human-readable description of the feed format itself"
    ),
    rights=T.str.optional.desc("A human-readable copyright statement for the feed"),
    rights_detail=Detailed.optional.desc(
        "A human-readable copyright statement for the feed"
    ),
    publisher=T.str.optional.desc("The publisher of the feed"),
    publisher_detail=UserInfo.optional.desc("The publisher of the feed"),
    image=T.dict(
        title=T.str.optional.desc("The alternate text of the feed image"),
        href=T.str.optional.desc("The URL of the feed image itself"),
        width=T.int.optional.desc("The width of the feed image"),
        height=T.int.optional.desc("The height of the feed image"),
        link=T.str.optional.desc("The URL which the feed image would point to"),
    ).optional.desc(
        "Details about the feed image. A feed image "
        "can be a logo, banner, or a picture of the author"
    ),
    generator=T.str.optional.desc(
        "A human-readable name of the application used to generate the feed"
    ),
    generator_detail=T.dict(
        name=T.str.optional.desc("Same as feed.generator"),
        href=T.str.optional.desc(
            "The URL of the application used to generate the feed"
        ),
        version=T.str.optional.desc(
            "The version number of the application used to generate the feed"
        ),
    ).optional.desc("Details about the feed generator"),
)


OPMLSchema = T.dict(
    title=T.str.optional,
    items=T.list(
        T.dict(
            title=T.str.optional,
            type=T.str.optional,
            url=T.url.optional,
        )
    )
)


validate_feed = compiler.compile(FeedSchema)
validate_story = compiler.compile(StorySchema)
validate_opml = compiler.compile(OPMLSchema)
