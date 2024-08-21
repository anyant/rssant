"""
Proper way to intercept stdlib logging:
    https://github.com/Delgan/loguru/issues/78
"""

import io
import logging
import os.path
import sys
import traceback

from loguru import logger
from loguru._logger import parse_ansi

from rssant_common.attrdict import AttrDict


class InterceptHandler(logging.Handler):
    def emit(self, record):
        logger_opt = logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


if hasattr(sys, '_getframe'):

    def currentframe():
        return sys._getframe(3)

else:  # pragma: no cover

    def currentframe():
        """Return the frame object for the caller's stack frame."""
        try:
            raise Exception
        except Exception:
            return sys.exc_info()[2].tb_frame.f_back


_srcfile = (
    logging._srcfile,
    os.path.normcase(parse_ansi.__code__.co_filename),
    os.path.normcase(currentframe.__code__.co_filename),
)


def findCaller(stack_info=False):
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.
    """
    f = currentframe()
    # On some versions of IronPython, currentframe() returns None if
    # IronPython isn't run with -X:Frames.
    if f is not None:
        f = f.f_back
    rv = "(unknown file)", 0, "(unknown function)", None
    while hasattr(f, "f_code"):
        co = f.f_code
        filename = os.path.normcase(co.co_filename)
        if filename in _srcfile:
            f = f.f_back
            continue
        sinfo = None
        if stack_info:
            sio = io.StringIO()
            sio.write('Stack (most recent call last):\n')
            traceback.print_stack(f, file=sio)
            sinfo = sio.getvalue()
            if sinfo[-1] == '\n':
                sinfo = sinfo[:-1]
            sio.close()
        rv = (co.co_filename, f.f_lineno, co.co_name, sinfo)
        break
    try:
        name = f.f_globals["__name__"]
    except KeyError:
        name = rv[3]
    return (name, *rv)


def fixed_get_frame(depth=None):
    try:
        name, fn, lno, func, sinfo = findCaller(stack_info=False)
    except ValueError:  # pragma: no cover
        fn, lno, func = "(unknown file)", 0, "(unknown function)"
        name = func
    frame = AttrDict(
        f_globals=dict(__name__=name),
        f_lineno=lno,
        f_code=AttrDict(
            co_filename=fn,
            co_name=func,
        ),
    )
    return frame


def loguru_patch():
    import loguru._get_frame
    import loguru._logger

    loguru._get_frame.get_frame = fixed_get_frame
    loguru._logger.get_frame = fixed_get_frame
