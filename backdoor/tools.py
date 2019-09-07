import time
import os
import gc
import sys
import linecache
import tracemalloc
import threading
import traceback
from collections import defaultdict

import objgraph
import pandas as pd
from pympler import muppy, summary

from .helper import shorten
from .asyncio_tools import get_event_loops, format_async_stack, get_all_tasks


def get(name):
    objs = objgraph.by_type(name)
    if objs:
        return objs[0]
    return None


def print_top_stats(top_stats, limit=10):
    print("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        # replace "/path/to/module/file.py" with "module/file.py"
        filename = os.sep.join(frame.filename.split(os.sep)[-2:])
        print("#%s: %s:%s: %.1f KiB"
              % (index, filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print('    %s' % line)
    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))


def top_diff(seconds=10, key_type='lineno', limit=10):
    tracemalloc.start()
    snapshot1 = tracemalloc.take_snapshot()
    time.sleep(seconds)
    snapshot2 = tracemalloc.take_snapshot()
    top_stats = snapshot2.compare_to(snapshot1, key_type)
    print_top_stats(top_stats, limit=limit)


def top(seconds=10, key_type='lineno', limit=10):
    tracemalloc.start()
    time.sleep(seconds)
    snapshot = tracemalloc.take_snapshot()
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)
    print_top_stats(top_stats, limit=limit)


def gc_top(seconds=10, limit=10):
    gc.collect()
    gc.set_debug(gc.DEBUG_LEAK)
    time.sleep(seconds)
    stats = defaultdict(lambda: 0)
    for x in gc.garbage:
        stats[type(x).__qualname__] += 1
    stats = list(reversed(sorted((v, k) for k, v in stats.items())))[:limit]
    for x in stats:
        print(*x)
    gc.set_debug(False)
    gc.garbage.clear()
    gc.collect()


def top_types(types=None):
    all_objects = muppy.get_objects()
    summary.print_(summary.summarize(all_objects))
    if types is not None:
        if isinstance(types, (list, tuple, set)):
            types = list(types)
        else:
            types = [types]
    else:
        types = []
    for t in types:
        print('###' + repr(t))
        objs = muppy.filter(all_objects, Type=t)
        top10 = list(reversed(sorted(objs, key=len)))[:10]
        for x in top10:
            print(len(x), shorten(repr(x), 60))


def _super_len(x):
    try:
        return len(x)
    except Exception:
        return 0


def _get_name(x):
    try:
        return x.__qualname__
    except Exception:
        try:
            return x.__name__
        except Exception:
            return None


try:
    from pytz.tzinfo import BaseTzInfo
except Exception:
    BaseTzInfo = None


def _is_pytz(x):
    if BaseTzInfo is None:
        return False
    return isinstance(x, BaseTzInfo)


def _get_type_name(x):
    if _is_pytz(x):
        return 'TzInfo'
    return _get_name(type(x))


def _get_module(x):
    try:
        return type(x).__module__
    except Exception:
        return None


def df_types(objects=None):
    if objects is None:
        objects = gc.get_objects()
    items = []
    for x in objects:
        mod = _get_module(x)
        type_name = mod + '.' + _get_type_name(x)
        items.append((
            mod,
            type_name,
            _super_len(x),
            sys.getsizeof(x, 0)
        ))
    df = pd.DataFrame(items, columns=['module', 'type', 'len', 'size'])
    df_count = df[['type']].groupby('type').size().reset_index(name='count')
    df_sum = df[['type', 'len', 'size']].groupby('type').sum().reset_index()
    df = df_count.merge(df_sum, on='type')
    df = df.sort_values(['count', 'len', 'size'], ascending=False)
    return df


def print_stack():
    # http://xiaorui.cc/2018/05/21/打印python线程stack分析当前上下文/
    print("\n*** STACKTRACE - START ***\n")
    for th in threading.enumerate():
        print(th)
        traceback.print_stack(sys._current_frames()[th.ident])
        print("\n")
    print("\n*** STACKTRACE - END ***\n")


def _print_async_tasks_stack(loop, thread):
    # https://mozillazg.com/2017/12/python-get-concurrency-programm-all-tracebacks-threading-gevent-asyncio-etc.html#hidasyncio-task-traceback
    tasks = get_all_tasks(loop)
    print(thread)
    print(loop)
    print('total {} tasks\n'.format(len(tasks)))
    for task in tasks:
        print(format_async_stack(task))


def print_async_stack():
    loops = get_event_loops()
    print("\n*** STACKTRACE - START ***\n")
    for i, (loop, thread) in enumerate(loops):
        if i != 0:
            print('-' * 79 + '\n')
        _print_async_tasks_stack(loop, thread)
    print("\n*** STACKTRACE - END ***\n")
