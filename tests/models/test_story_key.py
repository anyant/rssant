from rssant_api.models.story_storage.common.story_key import StoryId, hash_feed_id


def test_hash_feed_id():
    for i in [0, 1, 2, 7, 1024, 2**31, 2**32 - 1]:
        val = hash_feed_id(i)
        assert val >= 0 and val < 2**32


def test_story_id():
    cases = [
        (123, 10, 0x7b000000a0),
        (123, 1023, 0x7b00003ff0),
    ]
    for feed_id, offset, story_id in cases:
        assert StoryId.encode(feed_id, offset) == story_id
        assert StoryId.decode(story_id) == (feed_id, offset)
