import pytest
from rssant_api.models.story_unique_ids import StoryUniqueIdsData


CASES = {
    'empty': (0, []),
    'one': (1, [
        '93C07B6C-D848-4405-A349-07A3775FA0A9',
    ]),
    'two': (3, [
        'https://www.example.com/2.html',
        'https://www.example.com/3.html',
    ])
}


@pytest.mark.parametrize('case_name', list(CASES))
def test_encode_decode(case_name):
    begin_offset, unique_ids = CASES[case_name]
    data = StoryUniqueIdsData(begin_offset, unique_ids=unique_ids)
    data_bytes = data.encode()
    got = StoryUniqueIdsData.decode(data_bytes)
    assert got.begin_offset == begin_offset
    assert got.unique_ids == unique_ids
