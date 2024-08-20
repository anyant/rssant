import logging
import time
from importlib import import_module

from django.conf import settings

LOG = logging.getLogger(__name__)


def django_clear_expired_sessions():
    """
    see also: django/contrib/sessions/management/commands/clearsessions.py
    """
    begin_time = time.time()
    engine = import_module(settings.SESSION_ENGINE)
    try:
        engine.SessionStore.clear_expired()
    except NotImplementedError:
        msg = (
            "Session engine '%s' doesn't support clearing "
            "expired sessions." % settings.SESSION_ENGINE
        )
        LOG.info(msg)
    else:
        cost = time.time() - begin_time
        LOG.info('django_clear_expired_sessions cost {:.1f}ms'.format(cost * 1000))
