import os.path
import gc
import asyncio
import linecache
import traceback
import threading
from asyncio import BaseEventLoop


# asyncio.Task.all_tasks is deprecated and will be removed in Python 3.9
if hasattr(asyncio, 'all_tasks'):
    get_all_tasks = asyncio.all_tasks
else:
    get_all_tasks = asyncio.Task.all_tasks


# This is borrowed from curio/task.py
# Internal functions used for debugging/diagnostics
def _get_stack(coro):
    '''
    Extracts a list of stack frames from a chain of generator/coroutine calls
    '''
    frames = []
    while coro:
        if hasattr(coro, 'cr_frame'):
            f = coro.cr_frame
            coro = coro.cr_await
        elif hasattr(coro, 'ag_frame'):
            f = coro.ag_frame
            coro = coro.ag_await
        elif hasattr(coro, 'gi_frame'):
            f = coro.gi_frame
            coro = coro.gi_yieldfrom
        else:
            # Note: Can't proceed further.  Need the ags_gen or agt_gen attribute
            # from an asynchronous generator.  See https://bugs.python.org/issue32810
            f = None
            coro = None

        if f is not None:
            frames.append(f)
    return frames


# Create a stack traceback for a task
def _format_stack(task, coro, complete=False):
    '''
    Formats a traceback from a stack of coroutines/generators
    '''
    dirname = os.path.dirname(__file__)
    extracted_list = []
    checked = set()
    for f in _get_stack(coro):
        lineno = f.f_lineno
        co = f.f_code
        filename = co.co_filename
        name = co.co_name
        if not complete and os.path.dirname(filename) == dirname:
            continue
        if filename not in checked:
            checked.add(filename)
            linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        extracted_list.append((filename, lineno, name, line))
    if not extracted_list:
        resp = 'No stack for %r' % task
    else:
        resp = 'Stack for %r (most recent call last):\n' % task
        resp += ''.join(traceback.format_list(extracted_list))
    return resp


def _asyncio_format_stack(task):
    # https://mozillazg.com/2017/12/python-get-concurrency-programm-all-tracebacks-threading-gevent-asyncio-etc.html#hidasyncio-task-traceback
    stack_list = []
    for stack in task.get_stack():
        stack_list.extend(
            traceback.format_list(traceback.extract_stack(stack))
        )
    return 'asyncio task {}:\n{}'.format(task, ''.join(stack_list))


def format_async_stack(task, complete=False):
    coro = getattr(task, '_coro', None)
    if coro is not None and asyncio.iscoroutine(coro):
        ret = _format_stack(task, coro, complete=complete)
    else:
        ret = _asyncio_format_stack(task)
    return ret.strip() + '\n'


def get_event_loops():
    loops = []
    threads = {x.ident: x for x in threading.enumerate()}
    for obj in gc.get_objects():
        if isinstance(obj, BaseEventLoop):
            thread_id = getattr(obj, '_thread_id', None)
            if thread_id is not None:
                thread = threads.get(thread_id)
            else:
                thread = None
            loops.append((obj, thread))
    return loops
