import logging
import asyncio
import queue
import time
from threading import Thread, RLock

from .client import AsyncActorClient
from .helper import unsafe_kill_thread
from .registery import ActorRegistery
from .storage import ActorStorageBase
from .message_queue import ActorMessageQueue


LOG = logging.getLogger(__name__)


class MessageSender:

    def __init__(
        self,
        registery: ActorRegistery,
        storage: ActorStorageBase,
        concurrency=100,
    ):
        self.registery = registery
        self.storage = storage
        self.concurrency = concurrency
        self.outbox = ActorMessageQueue(concurrency * 2)
        # message_id -> t_send
        self._send_message_states = {}
        self._thread = None
        self._lock = RLock()
        self._stop = False

    async def _poll_messages(self):
        highwater = max(1, self.concurrency // 10)
        messages = []

        def append(x):
            self.update_send_message_time(x.id, t_send)
            messages.append(x)

        while (not self._stop) and len(messages) <= highwater:
            t_send = time.time()
            try:
                append(self.outbox.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.1)
                try:
                    while (not self._stop) and len(messages) <= highwater:
                        append(self.outbox.get_nowait())
                except queue.Empty:
                    pass
                break
        return messages

    def update_send_message_time(self, message_id, t_send):
        with self._lock:
            self._send_message_states[message_id] = t_send

    def get_send_message_states(self):
        with self._lock:
            return self._send_message_states.copy()

    def remove_send_message_states(self, message_ids):
        with self._lock:
            for msg_id in message_ids:
                self._send_message_states.pop(msg_id, None)

    async def _main(self):
        client = AsyncActorClient(registery=self.registery)
        async with client:
            while not self._stop:
                try:
                    messages = await self._poll_messages()
                    if messages:
                        await client.send(*messages)
                        messages = []
                except Exception as ex:
                    LOG.exception(ex)

    def main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._main())

    async def async_submit(self, message):
        LOG.info(f'submit message {message}')
        return await self.outbox.async_put(message)

    def submit(self, message):
        LOG.info(f'submit message {message}')
        return self.outbox.put(message)

    def start(self):
        self._thread = Thread(target=self.main, name='actor_message_sender')
        self._thread.daemon = True
        self._thread.start()

    def shutdown(self):
        self._stop = True
        if self._thread and self._thread.is_alive():
            unsafe_kill_thread(self._thread.ident)

    def join(self):
        if self._thread:
            self._thread.join()
