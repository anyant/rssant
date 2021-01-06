from requests import Response
from requests.exceptions import ChunkedEncodingError


def requests_check_incomplete_response(response: Response):
    """
    Check that we have read all the data as the requests library does not
    currently enforce this.
    https://blog.petrzemek.net/2018/04/22/on-incomplete-http-reads-and-the-requests-library-in-python/
    """
    expected_length = response.headers.get('Content-Length')
    if expected_length is not None:
        actual_length = response.raw.tell()
        expected_length = int(expected_length)
        if actual_length < expected_length:
            msg = 'incomplete response ({} bytes read, {} more expected)'.format(
                actual_length, expected_length - actual_length)
            raise ChunkedEncodingError(msg, response=response)
