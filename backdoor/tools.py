import time
import linecache
import os
import tracemalloc
import gc
from collections import defaultdict
from pympler import muppy, summary

from .helper import shorten


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
