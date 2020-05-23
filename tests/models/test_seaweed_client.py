import pytest
from rssant_api.models.seaweed_client import SeaweedClient, SeaweedError


@pytest.mark.xfail(run=False, reason='depends on seaweed service')
def test_seaweed_client():
    volume_url = 'http://127.0.0.1:9080'
    client = SeaweedClient(volume_url)
    try:
        fid = '1,01637037d6'
        client.delete(fid)

        got = client.get(fid)
        assert got is None
        client.put(fid, b'hello world')
        got = client.get(fid)
        assert got == b'hello world'

        bad_fid = '1,01637037d7'
        assert client.get(bad_fid) is None
        with pytest.raises(SeaweedError):
            client.put(bad_fid, b'hello seaweed')

        client.delete(fid)
        got = client.get(fid)
        assert got is None
    finally:
        client.close()


def test_seaweed_client_context():
    volume_url = 'http://127.0.0.1:9080'
    with SeaweedClient(volume_url) as client:
        assert client
