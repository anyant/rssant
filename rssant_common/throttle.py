import functools
import time


class throttle:
    def __init__(self, seconds: int) -> None:
        self._seconds = seconds
        self._last_call_time = None

    def _check_call(self):
        now = time.time()
        if self._last_call_time is None:
            self._last_call_time = now
            return True
        if now - self._last_call_time >= self._seconds:
            self._last_call_time = now
            return True
        return False

    def __call__(self, func):
        state = self

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if state._check_call():
                func(self, *args, **kwargs)

        return wrapper
