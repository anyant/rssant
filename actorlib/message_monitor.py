import time
import asyncio
import logging
from threading import Thread

from .message import ActorMessage
from .sender import MessageSender
from .executor import ActorExecutor
from .storage import ActorStorageBase
from .state import ActorStateError
from .helper import unsafe_kill_thread
from .context import StorageHelper


LOG = logging.getLogger(__name__)


class ActorMessageMonitor:

    def __init__(
        self,
        storage: ActorStorageBase,
        executor: ActorExecutor,
        sender: MessageSender,
        ack_timeout=10 * 60,
        max_retry_count=3,
    ):
        self.storage = storage
        self.executor = executor
        self.sender = sender
        self.ack_timeout = ack_timeout
        self.max_retry_count = max_retry_count
        self.registery = executor.registery
        self._storage_helper = StorageHelper(self.storage, self.registery)
        self._thread = None
        self._stop = False

    def _get_retry_timeout(self, count):
        return (count + 1) / self.max_retry_count * self.ack_timeout

    async def _ack_error_notry(self, msg_id):
        ack_msg = self._storage_helper.execute_op(
            self.storage.op_ack, msg_id, 'ERROR_NOTRY')
        if ack_msg:
            await self.sender.async_submit(ack_msg)

    async def check_send_messages(self):
        send_messages = self.storage.query_send_messages()
        send_message_states = self.sender.get_send_message_states()
        acked_message_ids = set(send_message_states) - set(send_messages)
        self.sender.remove_send_message_states(acked_message_ids)
        retry_messages = []
        error_notry_messages = []
        now = time.time()
        t_timeout = now - self.ack_timeout
        # TODO: use time wheel algorithm
        for msg_id, (state, msg) in send_messages.items():
            t_send = send_message_states.get(msg_id, None)
            if state['count'] >= self.max_retry_count:
                error_notry_messages.append(msg_id)
                continue
            if t_send is None or t_send < t_timeout:
                # TODO: fix local recursion messages
                if self.registery.is_local_node(msg['dst_node']):
                    error_notry_messages.append(msg_id)
                else:
                    retry_messages.append((msg_id, msg))
                continue
            if state['status'] == 'ERROR':
                error_timeout = now - self._get_retry_timeout(state['count'])
                if t_send < error_timeout:
                    retry_messages.append((msg_id, msg))
                    continue
        self.sender.remove_send_message_states(error_notry_messages)
        self.sender.remove_send_message_states(
            [msg_id for msg_id, msg in retry_messages])
        if error_notry_messages:
            LOG.info(f'error_notry {len(error_notry_messages)} messages')
        for msg_id in error_notry_messages:
            await self._ack_error_notry(msg_id)
        if retry_messages:
            LOG.info(f'retry {len(retry_messages)} messages')
        for msg_id, msg in retry_messages:
            try:
                self.storage.op_retry(msg_id)
            except ActorStateError as ex:
                LOG.warning(ex)
            try:
                msg = ActorMessage.from_dict(msg)
            except KeyError as ex:
                LOG.exception(ex)
                continue
            if self.registery.is_local_message(msg):
                await self.executor.async_submit(msg)
            else:
                await self.sender.async_submit(msg)

    async def _main(self):
        while not self._stop:
            await asyncio.sleep(1)
            try:
                await self.check_send_messages()
            except Exception as ex:
                LOG.exception(ex)

    def main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._main())

    def start(self):
        self._thread = Thread(target=self.main, name='actor_message_monitor')
        self._thread.daemon = True
        self._thread.start()

    def shutdown(self):
        self._stop = True
        if self._thread and self._thread.is_alive():
            unsafe_kill_thread(self._thread.ident)

    def join(self):
        if self._thread:
            self._thread.join()
