from rest_framework.exceptions import APIException, ErrorDetail
from rest_framework.status import HTTP_400_BAD_REQUEST


class RssantAPIException(APIException):

    status_code = HTTP_400_BAD_REQUEST
    default_detail = 'Invalid request'
    default_code = 'invalid'

    def __init__(self, detail=None, code=None):
        if detail is None:
            detail = self.default_detail
        if code is None:
            code = self.default_code
        self.detail = ErrorDetail(detail, code)
