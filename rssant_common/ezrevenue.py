import logging
import secrets
import time

import jwt
import requests
import six

from rssant_config import CONFIG

LOG = logging.getLogger(__name__)

_TOKEN_ALGORITHM = "HS256"
_TOKEN_FIELDS = ["exp", "nonce"]
_TOKEN_EXPIRES_IN = 30 * 60

_BASE_URL = 'https://revenue.ezboti.com/api/v1/server'


class EzrevenueClient:
    def __init__(
        self,
        *,
        project_id: str,
        project_secret: str,
        base_url: str = None,
    ) -> None:
        self.project_id = project_id
        self.project_secret = project_secret
        self.base_url = base_url or _BASE_URL

    def decode_token(self, token: str) -> dict:
        try:
            payload = jwt.decode(
                token,
                key=self.project_secret,
                options={'require': _TOKEN_FIELDS},
                algorithms=[_TOKEN_ALGORITHM],
            )
        except jwt.InvalidTokenError as ex:
            raise ex
        return payload

    def encode_token(self, payload: dict) -> str:
        exp = int(time.time()) + int(_TOKEN_EXPIRES_IN)
        nonce = secrets.token_urlsafe(10)
        payload = dict(payload, exp=exp, nonce=nonce)
        token = jwt.encode(
            payload,
            key=self.project_secret,
            algorithm=_TOKEN_ALGORITHM,
            headers=dict(project_id=self.project_id),
        )
        # Note: pyjwt 2.0+ 返回 str 类型, 1.0+ 返回 bytes 类型
        return six.ensure_str(token)

    def call(self, api: str, params: dict):
        payload = {"method": api, "params": params}
        content = self.encode_token(payload)
        url = self.base_url + '/' + api
        response = requests.post(url, data=content)
        try:
            response.raise_for_status()
        except requests.HTTPError:
            err_msg = '%s failed status=%s, body=%s'
            LOG.warning(err_msg, api, response.status_code, response.text)
            raise
        result = self.decode_token(response.text)
        return result['result']


EZREVENUE_CLIENT = None
if CONFIG.ezrevenue_enable:
    EZREVENUE_CLIENT = EzrevenueClient(
        project_id=CONFIG.ezrevenue_project_id,
        project_secret=CONFIG.ezrevenue_project_secret,
        base_url=CONFIG.ezrevenue_base_url,
    )
