import logging
from collections import defaultdict, deque

from .message import ActorMessage
from .helper import format_timestamp


LOG = logging.getLogger(__name__)


INBOX = "INBOX"
EXECUTE = "EXECUTE"
OUTBOX = "OUTBOX"
EXPORT = "EXPORT"
OK = "OK"
ERROR = "ERROR"
ERROR_NOTRY = "ERROR_NOTRY"

MESSAGE_STATUS = (INBOX, EXECUTE, OUTBOX, OK, ERROR, ERROR_NOTRY)
OUTBOX_MESSAGE_STATUS = (OUTBOX, EXPORT, OK, ERROR, ERROR_NOTRY)


class ActorStateError(Exception):
    """ActorStateError"""


class DuplicateMessageError(ActorStateError):
    """Duplicate message error"""


class MessageNotExistsError(ActorStateError):
    """Message not in state or ack/retry message not in send messages"""


class InvalidStatusError(ActorStateError):
    """Invalid message status or ack status"""


class CompleteMessageStatus:
    """
    >>> s = CompleteMessageStatus(maxlen=3)
    >>> bool(s)
    False
    >>> s.add(1, OK)
    >>> s.add(2, OK)
    >>> s.add(3, ERROR)
    >>> s.add(4, ERROR)
    >>> len(s)
    3
    >>> 1 in s
    False
    >>> s[2] == OK
    True
    >>> s[3] == ERROR
    True
    >>> list(s) == [(2, OK), (3, ERROR), (4, ERROR)]
    True
    """

    def __init__(self, maxlen):
        self.maxlen = maxlen
        self._ids_status = {}
        self._ids_deque = deque()

    def add(self, message_id: str, status: str):
        if message_id in self._ids_status:
            raise DuplicateMessageError(f'message {message_id} already complete')
        self._ids_status[message_id] = status
        self._ids_deque.append(message_id)
        self._compact()

    def _compact(self):
        while len(self._ids_deque) > self.maxlen:
            message_id = self._ids_deque.popleft()
            self._ids_status.pop(message_id, None)

    def __getitem__(self, message_id) -> str:
        return self._ids_status[message_id]

    def __iter__(self):
        for messagd_id in self._ids_deque:
            yield (messagd_id, self._ids_status[messagd_id])

    def __contains__(self, message_id):
        return message_id in self._ids_status

    def __bool__(self):
        return bool(self._ids_deque)

    def __len__(self):
        return len(self._ids_deque)


class ActorState:
    def __init__(self, max_complete_size=128):
        self.max_complete_size = max_complete_size
        # message_id -> {status, outbox_states -> [{status, executed_count, retry_at}]}
        self.state = {}
        # id -> message
        self.message_objects = {}
        # id -> outbox_message
        self.outbox_message_objects = {}
        # dst -> src_node -> set{message_id}
        self.done_message_ids = defaultdict(lambda: defaultdict(set))
        # complete message ids
        self.complete_message_state = CompleteMessageStatus(maxlen=max_complete_size)
        # dst -> set{src_node}
        self.upstream = defaultdict(set)

    def stats(self):
        stats = defaultdict(lambda: 0)
        for state in self.state.values():
            status = state['status']
            stats['num_' + status.lower()] += 1
            if status == OUTBOX:
                for outbox_state in state['outbox_states'].values():
                    outbox_status = outbox_state['status']
                    stats['num_outbox_' + outbox_status.lower()] += 1
        num_upstream = 0
        for dst, state in self.upstream.items():
            num_upstream += len(state)
        stats.update(num_upstream=num_upstream)
        stats.update(num_outbox=len(self.outbox_message_objects))
        stats.update(num_non_complete=len(self.message_objects))
        stats.update(num_complete=len(self.complete_message_state))
        stats.update(wal_size=self.wal_size)
        acks = []
        num_done = 0
        for dst, d in self.done_message_ids.items():
            for src_node, v in d.items():
                if v:
                    num_done += len(v)
                    acks.append(dict(dst=dst, src_node=src_node, size=len(v)))
        acks = list(sorted(acks, key=lambda x: x['size']))
        stats.update(num_done=num_done, acks=acks)
        return stats

    @property
    def wal_size(self):
        n = len(self.complete_message_state)
        for state in self.state.values():
            status = state['status']
            if status in (INBOX, OK, ERROR, ERROR_NOTRY):
                n += 1
            elif status == EXECUTE:
                n += 2
            elif status == OUTBOX:
                n += 8
        for dst, state in self.upstream.items():
            n += len(state)
        return n

    def get_message(self, message_id: str) -> ActorMessage:
        msg = self.message_objects.get(message_id)
        if msg is None:
            raise MessageNotExistsError(f'message {message_id} not exists')
        return msg

    def get_outbox_message(self, outbox_message_id: str) -> ActorMessage:
        msg = self.outbox_message_objects.get(outbox_message_id)
        if msg is None:
            raise MessageNotExistsError(f'outbox_message {outbox_message_id} not exists')
        return msg

    def get_state(self, message_id: str):
        if message_id in self.complete_message_state:
            return dict(status=self.complete_message_state[message_id])
        return self.state.get(message_id)

    def get_outbox_state(self, outbox_message_id: str):
        outbox_message = self.outbox_message_objects.get(outbox_message_id)
        if outbox_message is None:
            return None
        message_id = outbox_message.parent_id
        state = self.state[message_id]
        outbox_state = state['outbox_states'][outbox_message.id]
        return outbox_state

    def apply(self, type, **kwargs):
        return getattr(self, f'apply_{type}')(**kwargs)

    def apply_notify(self, *, dst, src_node: str, available: bool):
        LOG.debug(f'apply_notify dst={dst} src_node={src_node} available={available}')
        state = self.upstream[dst]
        if available:
            state.add(src_node)
        else:
            state.discard(src_node)

    def apply_inbox(self, *, message: ActorMessage):
        LOG.debug(f'apply_inbox {message}')
        state = self.get_state(message.id)
        if state is not None:
            current_status = state['status']
            if current_status != ERROR and current_status != ERROR_NOTRY:
                raise DuplicateMessageError(
                    f'duplicate message {message.id} status={current_status}')
        self.state[message.id] = dict(status=INBOX)
        self.message_objects[message.id] = message

    def apply_execute(self, *, message_id: str):
        LOG.debug(f'apply_execute {message_id}')
        state = self.get_state(message_id)
        if state is None:
            raise MessageNotExistsError(f"message {message_id} not exists")
        current_status = state['status']
        if current_status != INBOX:
            raise InvalidStatusError(
                f'can not apply_execute status={current_status} message {message_id}')
        state.update(status=EXECUTE)
        self.message_objects[message_id] = self.get_message(message_id).meta()

    def apply_outbox(self, *, message_id: str, outbox_messages: ActorMessage):
        LOG.debug(f'apply_outbox {message_id} outbox_messages={outbox_messages}')
        if not outbox_messages:
            raise ActorStateError("can not apply_outbox empty messages")
        state = self.get_state(message_id)
        if state is None:
            raise MessageNotExistsError(f"message {message_id} not exists")
        current_status = state['status']
        if current_status != EXECUTE:
            raise InvalidStatusError(
                f'can not apply_outbox status={current_status} message {message_id}')
        for x in outbox_messages:
            if x.parent_id != message_id:
                raise ActorStateError(
                    f"message {message_id} outbox_message invalid parent {x.parent_id} ")
        outbox_states = {}
        for x in outbox_messages:
            outbox_states[x.id] = dict(status=OUTBOX, executed_count=0, retry_at=None)
            self.outbox_message_objects[x.id] = x
        state.update(status=OUTBOX, outbox_states=outbox_states)

    def apply_done(self, *, message_id: str, status: str):
        LOG.debug(f'apply_done {message_id} status={status}')
        if status not in (OK, ERROR, ERROR_NOTRY):
            raise InvalidStatusError(f'invalid done status {status}')
        state = self.get_state(message_id)
        if state is None:
            raise MessageNotExistsError(f"message {message_id} not exists")
        current_status = state['status']
        if current_status not in (EXECUTE, OUTBOX):
            raise ActorStateError(
                f"can not apply_done for message {message_id} status={current_status}")
        message = self.get_message(message_id)
        outbox_states = state.pop('outbox_states', None)
        if outbox_states:
            for outbox_message_id in outbox_states:
                self.outbox_message_objects.pop(outbox_message_id, None)
        state.update(status=status)
        self.done_message_ids[message.dst][message.src_node].add(message_id)
        if not message.require_ack:
            self.apply_complete(message_id=message_id)

    def apply_complete(self, *, message_id: str, status: str = None):
        LOG.debug(f'apply_complete {message_id} status={status}')
        if message_id in self.complete_message_state:
            raise ActorStateError(f"message {message_id} already complete")
        if message_id in self.state:
            state = self.state[message_id]
            current_status = state['status']
            if current_status not in (INBOX, OK, ERROR, ERROR_NOTRY):
                raise ActorStateError(
                    f"can not apply_complete for message {message_id} status={current_status}")
            if status is None:
                status = current_status
            if status not in (OK, ERROR, ERROR_NOTRY):
                raise InvalidStatusError(f'invalid complete status {status}')
            message = self.get_message(message_id)
            self.done_message_ids[message.dst][message.src_node].discard(message_id)
            self.message_objects.pop(message_id, None)
            self.state.pop(message_id, None)
            self.complete_message_state.add(message_id, status)
        else:
            if status not in (OK, ERROR, ERROR_NOTRY):
                raise InvalidStatusError(f'invalid complete status {status}')
            self.complete_message_state.add(message_id, status)

    def apply_export(self, *, outbox_message_id: str, retry_at: int = None):
        LOG.debug(f'apply_export outbox_message_id={outbox_message_id} '
                  f'retry_at={format_timestamp(retry_at)}')
        outbox_message = self.get_outbox_message(outbox_message_id)
        if outbox_message.require_ack and not retry_at:
            raise ActorStateError(
                f"retry_at is required for require_ack outbox_message {outbox_message_id}")
        message_id = outbox_message.parent_id
        state = self.get_state(message_id)
        if state is None:
            raise MessageNotExistsError(f"message {message_id} not exists")
        current_status = state['status']
        if current_status != OUTBOX:
            raise ActorStateError(
                f"can not apply_export for message {message_id} status={current_status}")
        outbox_state = state['outbox_states'][outbox_message.id]
        outbox_current_status = outbox_state['status']
        if outbox_current_status != OUTBOX:
            raise ActorStateError(
                f"can not apply_export for outbox_message {outbox_message.id} "
                f"status={outbox_current_status}")
        executed_count = outbox_state['executed_count'] + 1
        outbox_state.update(status=EXPORT, executed_count=executed_count, retry_at=retry_at)
        if not outbox_message.require_ack:
            self.apply_acked(outbox_message_id=outbox_message.id, status=OK)

    def _get_all_done_status(self, outbox_states):
        """
        not done -> None
        all ok -> OK
        else -> ERROR_NOTRY
        """
        has_error = False
        for outbox_message_id, outbox_state in outbox_states.items():
            status = outbox_state['status']
            if status == OK:
                continue
            if status == ERROR_NOTRY:
                has_error = True
                continue
            outbox_message = self.get_outbox_message(outbox_message_id)
            if status == ERROR:
                executed_count = outbox_state['executed_count']
                if executed_count > outbox_message.max_retry:
                    has_error = True
                    continue
            return None
        return ERROR_NOTRY if has_error else OK

    def apply_acked(self, *, outbox_message_id: str, status: str):
        LOG.debug(f'apply_acked outbox_message_id={outbox_message_id} status={status}')
        outbox_message = self.get_outbox_message(outbox_message_id)
        if status not in (OK, ERROR, ERROR_NOTRY):
            raise InvalidStatusError(f'invalid acked status {status}')
        message_id = outbox_message.parent_id
        state = self.get_state(message_id)
        if state is None:
            raise MessageNotExistsError(f'invalid or already acked message {message_id}')
        current_status = state['status']
        if current_status != OUTBOX:
            raise ActorStateError(
                f"can not apply_acked for message {message_id} status={current_status}")
        outbox_states = state['outbox_states']
        outbox_state = outbox_states[outbox_message.id]
        outbox_current_status = outbox_state['status']
        # OUTBOX: expired and self-acked  EXPORT: acked
        if outbox_current_status not in (OUTBOX, EXPORT):
            raise ActorStateError(
                f"can not apply_acked for outbox_message {outbox_message.id} "
                f"status={outbox_current_status}")
        outbox_state.update(status=status)
        # will execute all outbox messages before ack done status
        done_status = self._get_all_done_status(outbox_states)
        if done_status:
            self.apply_done(message_id=message_id, status=done_status)
        else:
            if status in (OK, ERROR_NOTRY):
                outbox_state.update(retry_at=None)
                self.outbox_message_objects[outbox_message.id] = outbox_message.meta()

    def apply_retry(self, *, outbox_message_id: str):
        LOG.debug(f'apply_retry outbox_message_id={outbox_message_id}')
        outbox_message = self.get_outbox_message(outbox_message_id)
        message_id = outbox_message.parent_id
        state = self.get_state(message_id)
        if state is None:
            raise MessageNotExistsError(f'invalid or already acked message {message_id}')
        current_status = state['status']
        if current_status != OUTBOX:
            raise ActorStateError(
                f"can not apply_retry for message {message_id} status={current_status}")
        outbox_states = state['outbox_states']
        outbox_state = outbox_states[outbox_message.id]
        outbox_current_status = outbox_state['status']
        if outbox_current_status not in (ERROR, EXPORT):
            raise ActorStateError(
                f"can not apply_retry for outbox_message {outbox_message.id} "
                f"status={outbox_current_status}")
        outbox_state.update(status=OUTBOX, retry_at=None)

    def apply_restart(self):
        LOG.debug(f'apply_restart')
        message_ids = []
        for message_id, state in self.state.items():
            if state['status'] == EXECUTE:
                message_ids.append(message_id)
            elif state['status'] == INBOX:
                message = self.get_message(message_id)
                if message.is_ask:
                    message_ids.append(message_id)
        for message_id in message_ids:
            self.apply_done(message_id=message_id, status=ERROR)

    def _dump_outbox_state(self, outbox_message_id, outbox_state):
        outbox_message = self.get_outbox_message(outbox_message_id)
        outbox_status = outbox_state['status']
        for i in range(outbox_state['executed_count'] - 1):
            yield dict(type='export', outbox_message_id=outbox_message_id, retry_at=-1)
            yield dict(type='retry', outbox_message_id=outbox_message_id)
        if outbox_status == EXPORT:
            retry_at = outbox_state['retry_at']
            yield dict(type='export', outbox_message_id=outbox_message_id, retry_at=retry_at)
        elif outbox_status in (OK, ERROR_NOTRY):
            yield dict(type='export', outbox_message_id=outbox_message_id, retry_at=-1)
            if outbox_message.require_ack:
                yield dict(type='acked', outbox_message_id=outbox_message_id, status=outbox_status)
        elif outbox_status == ERROR:
            retry_at = outbox_state['retry_at']
            yield dict(type='export', outbox_message_id=outbox_message_id, retry_at=retry_at)
            yield dict(type='acked', outbox_message_id=outbox_message_id, status=ERROR)

    def dump(self):
        for message_id, status in self.complete_message_state:
            yield dict(type='complete', message_id=message_id, status=status)
        for message_id, state in self.state.items():
            status = state['status']
            message = self.get_message(message_id)
            yield dict(type='inbox', message=message)
            if status in (OK, ERROR, ERROR_NOTRY):
                yield dict(type='execute', message_id=message_id)
                yield dict(type='done', message_id=message_id, status=status)
            elif status == EXECUTE:
                yield dict(type='execute', message_id=message_id)
            elif status == OUTBOX:
                yield dict(type='execute', message_id=message_id)
                outbox_states = state['outbox_states']
                outbox_messages = []
                for outbox_message_id in outbox_states.keys():
                    outbox_message = self.get_outbox_message(outbox_message_id)
                    outbox_messages.append(outbox_message)
                yield dict(type='outbox', message_id=message_id, outbox_messages=outbox_messages)
                for outbox_message_id, outbox_state in outbox_states.items():
                    yield from self._dump_outbox_state(outbox_message_id, outbox_state)
        for dst, state in self.upstream.items():
            for src_node in state:
                yield dict(type='notify', dst=dst, src_node=src_node, available=True)

    def get_inbox_messages(self):
        for message_id, state in self.state.items():
            if state['status'] == INBOX:
                yield self.get_message(message_id)

    def get_outbox_messages(self):
        for message_id, state in self.state.items():
            if state['status'] != OUTBOX:
                continue
            message = self.get_message(message_id)
            outbox_messages = []
            outbox_states = state['outbox_states']
            for outbox_message_id, outbox_state in outbox_states.items():
                outbox_status = outbox_state['status']
                if outbox_status == OUTBOX:
                    outbox_message = self.get_outbox_message(outbox_message_id)
                    outbox_messages.append(outbox_message)
            if outbox_messages:
                yield (message, outbox_messages)
