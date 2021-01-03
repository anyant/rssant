from django.conf import settings
from django.contrib.messages.storage.base import BaseStorage
from django.contrib.messages.storage.cookie import CookieStorage


class FakeMessageStorage(BaseStorage):
    """A fake messge storage to disable messages middleware"""

    def _get(self, *args, **kwargs):
        return [], True

    def _store(self, messages, response, *args, **kwargs):
        # see also: CookieStorage._store
        cookie_name = CookieStorage.cookie_name
        data = self.request.COOKIES.get(cookie_name)
        if data:
            response.delete_cookie(
                cookie_name, domain=settings.SESSION_COOKIE_DOMAIN)
        return []
