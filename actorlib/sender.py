import logging
import asyncio
import queue
import time
from threading import Thread, RLock

from .client import AsyncActorClient
from .helper import unsafe_kill_thread
from .registery import ActorRegistery
from .storage import ActorStorageBase
from .message import ActorMessage


LOG = logging.getLogger(__name__)


class MessageSender:

    def __init__(
        self,
        registery: ActorRegistery,
        storage: ActorStorageBase,
        ack_timeout=10 * 60,
        max_retry_count=3,
        concurrency=100,
    ):
        self.registery = registery
        self.storage = storage
        self.ack_timeout = ack_timeout
        self.max_retry_count = max_retry_count
        self.concurrency = concurrency
        self.outbox = queue.Queue(concurrency * 2)
        # message_id -> t_send
        self._send_message_states = {}
        self._thread = None
        self._monitor_thread = None
        self._lock = RLock()
        self._stop = False

    async def _poll_messages(self):
        highwater = max(1, self.concurrency // 10)
        messages = []

        def append(x):
            self._update_send_message(t_send, x)
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

    def _update_send_message(self, t_send, message):
        with self._lock:
            self._send_message_states[message.id] = t_send

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
        while True:
            try:
                return self.outbox.put_nowait(message)
            except queue.Full:
                await asyncio.sleep(0.1)

    def submit(self, message):
        return self.outbox.put(message)

    async def check_send_messages(self):
        send_messages = self.storage.query_send_messages()
        with self._lock:
            acked_message_ids = set(self._send_message_states) - set(send_messages)
            for msg_id in acked_message_ids:
                self._send_message_states.pop(msg_id, None)
            retry_messages = []
            error_notry_messages = []
            now = time.time()
            t_timeout = now - self.ack_timeout
            # TODO: use time wheel algorithm
            for msg_id, t_send in self._send_message_states.items():
                state, msg = send_messages[msg_id]
                if state['count'] >= self.max_retry_count:
                    error_notry_messages.append(msg_id)
                    continue
                if t_send < t_timeout:
                    retry_messages.append((msg_id, msg))
                    continue
                if state['status'] == 'ERROR':
                    error_timeout = now - (state['count'] + 1) / \
                        self.max_retry_count * self.ack_timeout
                    if t_send < error_timeout:
                        retry_messages.append((msg_id, msg))
                        continue
            for msg_id in error_notry_messages:
                self._send_message_states.pop(msg_id, None)
            for msg_id, msg in retry_messages:
                self._send_message_states.pop(msg_id, None)
        for msg_id in error_notry_messages:
            self.storage.op_ack(msg_id, 'ERROR_NOTRY')
        for msg_id, msg in retry_messages:
            self.storage.op_retry(msg_id)
            await self.async_submit(ActorMessage.from_dict(msg))

    async def check_done_messages(self):
        done_messages = self.storage.pop_done_messages()
        for msg_id, state in done_messages.items():
            await self.async_submit(ActorMessage(
                id=msg_id,
                src='actor.ack',
                dst='actor.ack',
                dst_node=state['src_node'],
                content=dict(status=state['status'])
            ))

    async def _monitor_main(self):
        while not self._stop:
            await asyncio.sleep(1)
            try:
                await self.check_send_messages()
                await self.check_done_messages()
            except Exception as ex:
                LOG.exception(ex)

    def monitor_main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._monitor_main())

    def start(self):
        self._thread = Thread(target=self.main)
        self._thread.daemon = True
        self._thread.start()
        self._monitor_thread = Thread(target=self.monitor_main)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()

    def shutdown(self):
        self._stop = True
        if self._thread and self._thread.is_alive():
            unsafe_kill_thread(self._thread.ident)
        if self._monitor_thread and self._monitor_thread.is_alive():
            unsafe_kill_thread(self._monitor_thread.ident)

    def join(self):
        if self._thread:
            self._thread.join()
        if self._monitor_thread:
            self._monitor_thread.join()
