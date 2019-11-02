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

from .server import BackdoorServer
from .helper import format_number
from .asyncio_tools import get_event_loops, format_async_stack, get_all_tasks


_INIT_MEMORY_SNAPSHOT = None


def setup():
    global _INIT_MEMORY_SNAPSHOT
    if tracemalloc.is_tracing():
        _INIT_MEMORY_SNAPSHOT = tracemalloc.take_snapshot()
    server = BackdoorServer()
    server.start()
    return server


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


def top_diff(key_type='lineno', limit=10):
    if _INIT_MEMORY_SNAPSHOT is None:
        print('tracemalloc not enabled')
        return
    snapshot2 = tracemalloc.take_snapshot()
    top_stats = snapshot2.compare_to(_INIT_MEMORY_SNAPSHOT, key_type)
    print_top_stats(top_stats, limit=limit)


def top(key_type='lineno', limit=10):
    snapshot = tracemalloc.take_snapshot()
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


def df_types(objects=None, limit=100, output=True):
    if objects is None:
        objects = gc.get_objects()
    items = []
    for x in objects:
        mod = _get_module(x)
        type_name = _get_type_name(x)
        items.append((
            mod,
            type_name,
            _super_len(x),
            sys.getsizeof(x, 0)
        ))
    results = defaultdict(lambda: [0, 0, 0])
    for mod, type_name, length, size in items:
        values = results[(mod, type_name)]
        values[0] += 1
        values[1] += length
        values[2] += size
    results = [(*k, *v) for k, v in results.items()]
    results = list(sorted(results, key=lambda x: x[2], reverse=True))
    if output:
        if isinstance(output, str):
            lines = ['module,type,count,length,size']
            for mod, type_name, count, length, size in results[:limit]:
                lines.append(f'{mod},{type_name},{count},{length},{size}')
            lines = '\n'.join(lines)
            if output == '-':
                print(lines)
            else:
                with open(output, 'w') as output_file:
                    output_file.write(lines)
        else:
            print('{:>30s} {:<35s} {:>6s} {:>6s} {:>6s}'.format(
                'module', 'type', 'count', 'length', 'size'))
            for mod, type_name, count, length, size in results[:limit]:
                print('{:>30s} {:<35s} {:>6s} {:>6s} {:>6s}'.format(
                    mod, type_name, format_number(count),
                    format_number(length), format_number(size)
                ))
    else:
        return results


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


if __name__ == "__main__":
    tracemalloc.start()
    setup()
