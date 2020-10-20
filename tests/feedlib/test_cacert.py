import pytest
from rssant_feedlib import cacert
from rssant_feedlib.cacert import _CacertHelper


def test_cacert():
    assert cacert.where()


@pytest.mark.xfail(run=False, reason='depends on test network')
def test_cacert_update():
    _CacertHelper.update()
