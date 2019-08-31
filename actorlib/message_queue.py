import queue
import logging

import asyncio


LOG = logging.getLogger(__name__)


class ActorMessageQueue(queue.Queue):

    def __init__(self, *args, async_retry_interval=0.1, **kwargs):
        super().__init__(*args, **kwargs)
        self.async_retry_interval = async_retry_interval

    async def async_get(self):
        while True:
            try:
                return self.get_nowait()
            except queue.Empty:
                await asyncio.sleep(self.async_retry_interval)

    async def async_put(self, message):
        while True:
            try:
                return self.put_nowait(message)
            except queue.Full:
                await asyncio.sleep(self.async_retry_interval)
