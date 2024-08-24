import functools
import time


class throttle:
    def __init__(self, seconds: int, _is_async=False) -> None:
        self._seconds = seconds
        self._is_async = _is_async
        self._last_call_time = None

    def _check_call(self):
        now = time.monotonic()
        if self._last_call_time is None:
            self._last_call_time = now
            return True
        if now - self._last_call_time >= self._seconds:
            self._last_call_time = now
            return True
        return False

    def _get_wrapper(self, func):
        state = self

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if state._check_call():
                func(self, *args, **kwargs)

        return wrapper

    def _get_async_wrapper(self, func):
        state = self

        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            if state._check_call():
                await func(self, *args, **kwargs)

        return wrapper

    def __call__(self, func):
        if self._is_async:
            wrapper = self._get_async_wrapper(func)
        else:
            wrapper = self._get_wrapper(func)
        return wrapper


class async_throttle(throttle):
    def __init__(self, seconds: int) -> None:
        super().__init__(seconds, _is_async=True)
