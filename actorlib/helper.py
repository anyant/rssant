import ctypes
import logging
import hashlib


LOG = logging.getLogger(__name__)


def shorten(text, width, placeholder='...'):
    """
    >>> shorten('123456789', width=8)
    '12345...'
    >>> shorten('123456789', width=9)
    '123456789'
    """
    if not text:
        return text
    if len(text) <= width:
        return text
    return text[: max(0, width - len(placeholder))] + placeholder


def unsafe_kill_thread(thread_id):
    # https://www.geeksforgeeks.org/python-different-ways-to-kill-a-thread/
    if thread_id is None:
        return False
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        thread_id, ctypes.py_object(SystemExit))
    if res > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
        LOG.error(f'kill thread#{thread_id} failed')
    return res <= 0


def hash_token(token):
    if not token:
        return None
    return hashlib.sha1(token.encode('utf-8')).hexdigest()
