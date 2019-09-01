import ctypes
import logging

from validr import T
from rssant_common.validator import compiler as internal_schema_compiler


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


parse_actor_timer = internal_schema_compiler.compile(
    T.interval.min('1s').max('24h'))
