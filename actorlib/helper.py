import ctypes
import logging
import functools
import asyncio
import time
import uuid
import datetime

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
    T.timedelta.min('1s').max('24h').object)


def _get_function_name(fn):
    mod_name = getattr(fn, '__module__', None)
    name = getattr(fn, '__qualname__', None)
    if not name:
        name = getattr(fn, '__name__', None)
    if mod_name:
        return f'{mod_name}.{name}'
    else:
        return name


def auto_restart_when_crash(fn):
    fn_name = _get_function_name(fn)
    if asyncio.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def wrapped(*args, **kwargs) -> None:
            while True:
                try:
                    await fn(*args, **kwargs)
                except Exception as ex:
                    LOG.error(f'{fn_name} crashed, will restart it', exc_info=ex)
                await asyncio.sleep(1)
    else:
        @functools.wraps(fn)
        def wrapped(*args, **kwargs) -> None:
            while True:
                try:
                    fn(*args, **kwargs)
                except Exception as ex:
                    LOG.error(f'{fn_name} crashed, will restart it', exc_info=ex)
                time.sleep(1)
    return wrapped


def generate_message_id(node_name):
    return node_name + ':' + str(uuid.uuid4())


def format_timestamp(t):
    if t is None:
        return None
    dt = datetime.datetime.utcfromtimestamp(t)
    return dt.isoformat(timespec='seconds') + 'Z'
