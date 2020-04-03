from pathlib import Path
from rssant_feedlib.processor import story_readability


def test_story_readability():
    """
    readability + lxml 4.5.0 has issue:
        readability.readability.Unparseable: IO_ENCODER
    """
    html_filepath = Path(__file__).parent / 'test_sample.html'
    with open(html_filepath) as f:
        html = f.read()
    story_readability(html)
