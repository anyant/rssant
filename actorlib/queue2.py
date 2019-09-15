import logging
import typing
import random
import heapq
import time
import asyncio
import threading

from .actor import Actor
from .message import ActorMessage
from .state2 import ActorState, OUTBOX, EXPORT, ERROR_NOTRY
from .storage2 import ActorStorage
from .helper import generate_message_id
from .builtin_actors.name import ACTOR_MESSAGE_FETCHER, ACTOR_MESSAGE_ACKER, ACTOR_MESSAGE_NOTIFY_SENDER


LOG = logging.getLogger(__name__)


class ActorStorageState:
    def __init__(self, storage, state):
        self._storage = storage
        self._state = state
        storage.load(state)

    def __getattr__(self, *args, **kwargs):
        return getattr(self._state, *args, **kwargs)

    def apply(self, type, **kwargs):
        self._state.apply(type, **kwargs)
        self._storage.save(type, **kwargs)

    def apply_notify(self, **kwargs):
        self.apply('notify', **kwargs)

    def apply_inbox(self, **kwargs):
        self.apply('inbox', **kwargs)

    def apply_execute(self, **kwargs):
        self.apply('execute', **kwargs)

    def apply_outbox(self, **kwargs):
        self.apply('outbox', **kwargs)

    def apply_done(self, **kwargs):
        self.apply('done', **kwargs)

    def apply_complete(self, **kwargs):
        self.apply('complete', **kwargs)

    def apply_export(self, **kwargs):
        self.apply('export', **kwargs)

    def apply_acked(self, **kwargs):
        self.apply('acked', **kwargs)

    def apply_retry(self, **kwargs):
        self.apply('retry', **kwargs)

    def apply_restart(self, **kwargs):
        self.apply('restart', **kwargs)


class ActorQueue:
    def __init__(
        self,
        node_name: str,
        actor_name: str,
        state: ActorState,
        schedule_fetcher,
        inbox_lowsize: int = 10,
        inbox_highsize: int = 30,
        outbox_lowsize: int = 100,
        outbox_highsize: int = 300,
        cycle_time: int = 10 * 60,
        max_retry_count: int = 3,
        max_retry_time: int = 10 * 60,
        fetcher_concurrency: int = 3,
    ):
        self.actor_name = actor_name
        self.node_name = node_name
        self.state = state
        self.schedule_fetcher = schedule_fetcher
        self.inbox_lowsize = inbox_lowsize
        self.inbox_highsize = inbox_highsize
        self.outbox_lowsize = outbox_lowsize
        self.outbox_highsize = outbox_highsize
        self.cycle_time = cycle_time
        self.max_retry_count = max_retry_count
        self.max_retry_time = max_retry_time
        self.fetcher_concurrency = fetcher_concurrency
        self.inbox = []  # [(priority, message)]
        self.dst_outbox = {}  # dst -> [(priority, message)]
        self.dst_node_outbox = {}  # dst_node -> dst -> [(priority, message)]
        self.is_fetching = False

    def __repr__(self):
        return '<{} {}>'.format(type(self).__name__, self.actor_name)

    def inbox_size(self):
        return len(self.inbox)

    def outbox_size(self):
        n = sum(len(x) for x in self.dst_outbox.values())
        for box in self.dst_node_outbox.values():
            n += sum(len(x) for x in box.values())
        for message_ids in self.state.done_message_ids[self.actor_name].values():
            n += len(message_ids)
        return n

    def is_inbox_empty(self):
        return self.inbox_size() <= 0

    def is_outbox_full(self):
        return self.outbox_size() >= self.outbox_highsize

    def execute_priority(self):
        priority, message = self.inbox[0]
        if priority is None:
            priority = 100
        return priority * (self.outbox_size() / self.outbox_highsize)

    def op_notify(self, dst: str, src_node: str, available: bool):
        self.state.apply_notify(dst=dst, src_node=src_node, available=available)
        self.auto_schedule_fetcher()

    def op_inbox(self, message: ActorMessage):
        if message.is_expired():
            LOG.warning(f'expired message {message}')
            return
        self.state.apply_inbox(message=message)
        heapq.heappush(self.inbox, (message.priority, message))

    def op_execute(self) -> ActorMessage:
        while True:
            priority, message = heapq.heappop(self.inbox)
            self.state.apply_execute(message_id=message.id)
            if message.is_expired():
                LOG.warning(f'expired message {message}')
                self.state.apply_complete(message_id=message.id, status=ERROR_NOTRY)
                continue
            self.auto_schedule_fetcher()
            return message

    def op_outbox(self, message_id: str, outbox_messages: [ActorMessage]):
        self.state.apply_outbox(message_id=message_id, outbox_messages=outbox_messages)
        for x in outbox_messages:
            self.push_outbox(x)

    def _export_box(self, result, box, retry_base_at):
        priority, outbox_message = heapq.heappop(box)
        if outbox_message.is_expired():
            LOG.warning(f'expired outbox_message {outbox_message}')
            self.state.apply_acked(outbox_message_id=outbox_message.id, status=ERROR_NOTRY)
        else:
            outbox_state = self.state.get_outbox_state(outbox_message.id)
            executed_count = outbox_state['executed_count']
            retry_at = retry_base_at + self.backoff_delay(executed_count)
            self.state.apply_export(outbox_message_id=outbox_message.id, retry_at=retry_at)
            result.append(outbox_message)

    def op_export(self, dst, dst_node, maxsize) -> [ActorMessage]:
        ret = []
        retry_base_at = time.time() + self.cycle_time
        dst_box = self.dst_node_outbox.get(dst_node)
        box = dst_box[dst] if dst_box else None
        while len(ret) < maxsize and box:
            self._export_box(ret, box, retry_base_at)
        box = self.dst_outbox.get(dst)
        while len(ret) < maxsize and box:
            self._export_box(ret, box, retry_base_at)
        self.auto_schedule_fetcher()
        return ret

    def op_done(self, message_id: str, status: str):
        self.state.apply_done(message_id=message_id, status=status)
        self.auto_schedule_fetcher()

    def on_fetcher_done(self):
        self.is_fetching = False
        self.auto_schedule_fetcher()

    def op_acked(self, outbox_message_id: str, status: str):
        self.state.apply_acked(outbox_message_id=outbox_message_id, status=status)
        self.auto_schedule_fetcher()

    def push_outbox(self, outbox_message):
        if outbox_message.dst_node:
            outbox = self.dst_node_outbox.setdefault(outbox_message.dst_node, {})
            outbox = outbox.setdefault(outbox_message.dst, [])
        else:
            outbox = self.dst_outbox.setdefault(outbox_message.dst, [])
        heapq.heappush(outbox, (outbox_message.priority, outbox_message))

    def outbox_info(self):
        dst_info = []
        dst_node_info = []
        for dst, box in self.dst_outbox.items():
            if box:
                dst_info.append(dst)
        for dst_node, dst_box in self.dst_node_outbox.items():
            for dst, box in dst_box.items():
                if box:
                    dst_node_info.append((dst_node, dst))
        return dst_info, dst_node_info

    def choice_available_upstream_list(self):
        nodes = self.state.upstream.get(self.actor_name, set())
        if len(nodes) <= self.fetcher_concurrency:
            return nodes
        return random.sample(nodes, self.fetcher_concurrency)

    def auto_schedule_fetcher(self):
        if self.is_fetching:
            return
        if self.outbox_size() > self.outbox_highsize:
            return
        if self.inbox_size() > self.inbox_lowsize:
            return
        upstream_list = self.choice_available_upstream_list()
        if not upstream_list:
            return
        maxsize = self.inbox_highsize - self.inbox_size()
        message_fetcher = ActorMessage(
            id=generate_message_id(self.node_name),
            priority=0,
            src=self.actor_name,
            src_node=self.node_name,
            dst=ACTOR_MESSAGE_FETCHER,
            dst_node=self.node_name,
            require_ack=False,
            content=dict(upstream_list=upstream_list, maxsize=maxsize, actor_name=self.actor_name),
        )
        self.schedule_fetcher(message_fetcher)
        self.is_fetching = True

    def backoff_delay(self, executed_count):
        # 8s, 64s, 8m, ...
        random_seconds = random.randint(0, 8 * 1000) / 1000
        return min(((8**executed_count) + random_seconds), self.max_retry_time)

    def check_timeout_and_retry(self, now):
        retry_outbox_message_ids = []
        error_notry_outbox_message_ids = []
        for state in self.state.state.values():
            if state['status'] != OUTBOX:
                continue
            for outbox_message_id, outbox_state in state['outbox_states'].items():
                outbox_status = outbox_state['status']
                retry_at = outbox_state.get('retry_at')
                if outbox_status == EXPORT and retry_at and retry_at < now:
                    executed_count = outbox_state['executed_count']
                    if executed_count >= self.max_retry_count - 1:
                        error_notry_outbox_message_ids.append(outbox_message_id)
                    else:
                        retry_outbox_message_ids.append(outbox_message_id)
        for outbox_message_id in error_notry_outbox_message_ids:
            self.op_acked(outbox_message_id, ERROR_NOTRY)
        for outbox_message_id in retry_outbox_message_ids:
            outbox_message = self.state.get_message(outbox_message_id)
            if outbox_message.is_expired():
                LOG.warning(f'expired outbox_message {outbox_message}')
                self.state.apply_acked(outbox_message_id=outbox_message.id, status=ERROR_NOTRY)
            else:
                self.state.apply_retry(outbox_message_id=outbox_message.id)
                self.push_outbox(outbox_message)
        return len(error_notry_outbox_message_ids)


class ActorMessageQueue:
    def __init__(
        self,
        node_name: str,
        actors: typing.Dict[str, Actor],
        storage: ActorStorage = None,
    ):
        self.node_name = node_name
        self.actors = actors
        state = ActorState()
        if storage:
            state = ActorStorageState(storage, state)
        self.state = state
        self.thread_actor_queues = {}
        self.async_actor_queues = {}
        self.lock = threading.Lock()
        self.execute_condition = threading.Condition(self.lock)
        self.is_notifing = False

    def actor_queue(self, actor_name: str):
        if actor_name not in self.actors:
            raise ValueError(f'actor {actor_name} not exists')
        actor = self.actors[actor_name]
        if actor.is_async:
            q = self.async_actor_queues.get(actor_name)
        else:
            q = self.thread_actor_queues.get(actor_name)
        if q is None:
            q = ActorQueue(

                node_name=self.node_name,
                actor_name=actor_name,
                state=self.state,
                schedule_fetcher=self._op_inbox,
            )
            if actor.is_async:
                self.async_actor_queues[actor_name] = q
            else:
                self.thread_actor_queues[actor_name] = q
        return q

    def all_actor_queues(self):
        for actor_queues in [self.async_actor_queues, self.thread_actor_queues]:
            yield from actor_queues.values()

    def inbox_size(self):
        return sum(x.inbox_size() for x in self.all_actor_queues())

    def outbox_size(self):
        return sum(x.outbox_size() for x in self.all_actor_queues())

    def qsize(self):
        self.inbox_size() + self.outbox_size()

    def op_execute(self) -> ActorMessage:
        """
        For executors
        """
        with self.execute_condition:
            while True:
                msg = self._op_execute(self.thread_actor_queues)
                if msg is not None:
                    return msg
                self.execute_condition.wait()

    async def async_op_execute(self) -> ActorMessage:
        while True:
            with self.lock:
                msg = self._op_execute(self.async_actor_queues)
                if msg is not None:
                    return msg
            await asyncio.sleep(0.1)

    def op_outbox(self, message_id: str, outbox_messages: [ActorMessage]):
        """
        For executors
        """
        with self.lock:
            message = self.state.get_message(message_id)
            if not message:
                LOG.warning(f'message {message_id} not exists')
                return
            self.actor_queue(message.dst).op_outbox(message_id, outbox_messages=outbox_messages)

    def op_done(self, message_id: str, status: str):
        """
        For executors
        """
        with self.lock:
            message = self.state.get_message(message_id)
            if not message:
                LOG.warning(f'message {message_id} not exists')
                return
            self.actor_queue(message.dst).op_done(message_id, status=status)
            if message.dst == ACTOR_MESSAGE_FETCHER:
                self.actor_queue(message.src).on_fetcher_done()
            if message.dst == ACTOR_MESSAGE_NOTIFY_SENDER:
                self.is_notifing = False
            self.execute_condition.notify()

    def op_export(self, dst: str, dst_node: str, maxsize: int):
        """
        For receiver (message exporter)
        """
        with self.lock:
            if dst == ACTOR_MESSAGE_ACKER:
                ret = list(self._export_ack(dst_node, maxsize))
            else:
                ret = []
                for actor_queue in self.all_actor_queues():
                    ret.extend(actor_queue.op_export(dst, dst_node, maxsize))
                    maxsize -= len(ret)
                    if maxsize <= 0:
                        break
            self.execute_condition.notify(len(ret))
            return ret

    def op_notify(self, src_node: str, dst: str, available: bool):
        """
        For upstream notify or message fetcher
        """
        with self.lock:
            self.actor_queue(dst).op_notify(dst=dst, src_node=src_node, available=available)
            self.execute_condition.notify()

    def op_inbox(self, message: ActorMessage):
        """
        For message fetcher or receiver
        """
        with self.lock:
            self._op_inbox(message)

    def op_acked(self, outbox_message_id: ActorMessage, status: str):
        """
        For message fetcher
        """
        with self.lock:
            outbox_message = self.state.get_message(outbox_message_id)
            if not outbox_message:
                LOG.warning(f'outbox_message {outbox_message_id} not exists')
                return
            message = self.state.get_message(outbox_message.parent_id)
            if not message:
                LOG.warning(f'message {outbox_message.parent_id} not eixsts')
                return
            self.actor_queue(message.dst).op_acked(outbox_message_id, status=status)
            self.execute_condition.notify()

    def op_tick(self, now: int):
        """
        For message monitor
        """
        with self.lock:
            self._auto_schedule_notifier()
            for actor_queue in self.all_actor_queues():
                num_error_notry = actor_queue.check_timeout_and_retry(now)
                if num_error_notry > 0:
                    self.execute_condition.notify(num_error_notry)

    def op_restart(self):
        """
        For application
        """
        with self.lock:
            self.state.apply_restart()

    def _op_inbox(self, message):
        self.actor_queue(message.dst).op_inbox(message)
        self.execute_condition.notify()

    def _ack_of(self, message, status):
        return ActorMessage(
            id=message.id,
            priority=0,
            src=message.dst,
            src_node=self.node_name,
            dst=ACTOR_MESSAGE_ACKER,
            dst_node=message.src_node,
            require_ack=False,
            content=dict(status=status),
        )

    def _export_ack(self, src_node, maxsize):
        message_and_status = []
        for dst, data in self.state.done_message_ids.items():
            for message_id in data.get(src_node, []):
                status = self.state.state[message_id]['status']
                message = self.state.get_message(message_id)
                message_and_status.append((message, status))
                maxsize -= 1
                if maxsize <= 0:
                    break
        for message, status in message_and_status:
            self.state.apply_complete(message_id=message.id)
            yield self._ack_of(message, status)

    def _auto_schedule_notifier(self):
        if self.is_notifing:
            return
        dst_info = set()
        dst_node_info = set()
        for actor_queue in self.all_actor_queues():
            dst_s, dst_node_s = actor_queue.outbox_info()
            dst_info.update(dst_s)
            dst_node_info.update(dst_node_s)
        for dst, dst_node_data in self.state.done_message_ids.items():
            for dst_node, items in dst_node_data.items():
                if items:
                    dst_node_info.add((dst_node, ACTOR_MESSAGE_ACKER))
        if not dst_info and not dst_node_info:
            return
        dst_info = [dict(dst=dst) for dst in dst_info]
        dst_node_info = [dict(dst=dst, dst_node=dst_node) for dst_node, dst in dst_node_info]
        message_notifier = ActorMessage(
            id=generate_message_id(self.node_name),
            priority=0,
            src='actor.init',
            src_node=self.node_name,
            dst=ACTOR_MESSAGE_NOTIFY_SENDER,
            dst_node=self.node_name,
            require_ack=False,
            content=dict(dst_info=dst_info, dst_node_info=dst_node_info),
        )
        self.actor_queue(message_notifier.dst).op_inbox(message_notifier)
        self.is_notifing = True

    def _op_execute(self, actor_queues):
        min_priority, min_actor = None, None
        for actor in actor_queues.values():
            if actor.is_inbox_empty() or actor.is_outbox_full():
                continue
            priority = actor.execute_priority()
            if min_priority is None or priority < min_priority:
                min_actor = actor
        if min_actor is not None:
            return min_actor.op_execute()
        return None
