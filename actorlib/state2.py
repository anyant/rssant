import logging
from collections import defaultdict

from .message import ActorMessage


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


class ActorState:
    def __init__(self):
        # message_id -> {status, is_acked, outbox_states -> [{status, executed_count, retry_at}]}
        self.state = {}
        # id -> message
        self.message_objects = {}
        # dst -> src_node -> set{message_id}
        self.done_message_ids = defaultdict(lambda: defaultdict(set))
        # dst -> set{src_node}
        self.upstream = defaultdict(set)

    def get_message(self, message_id: str) -> ActorMessage:
        return self.message_objects.get(message_id)

    def get_outbox_state(self, outbox_message_id: str):
        outbox_message = self.message_objects[outbox_message_id]
        message_id = outbox_message.parent_id
        state = self.state.get(message_id)
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
        state = self.state.get(message.id)
        if state is not None:
            current_status = state['status']
            if current_status != ERROR and current_status != ERROR_NOTRY:
                raise DuplicateMessageError(
                    f'duplicate message {message.id} status={current_status}')
        self.state[message.id] = dict(status=INBOX)
        self.message_objects[message.id] = message

    def apply_execute(self, *, message_id: str):
        LOG.debug(f'apply_execute {message_id}')
        state = self.state.get(message_id)
        if state is None:
            raise MessageNotExistsError(f"message {message_id} not exists")
        current_status = state['status']
        if current_status != INBOX:
            raise InvalidStatusError(
                f'can not apply_execute status={current_status} message {message_id}')
        state.update(status=EXECUTE)
        self.message_objects[message_id] = self.message_objects[message_id].meta()

    def apply_outbox(self, *, message_id: str, outbox_messages: ActorMessage):
        LOG.debug(f'apply_outbox {message_id} outbox_messages={outbox_messages}')
        if not outbox_messages:
            raise ActorStateError("can not apply_outbox empty messages")
        state = self.state.get(message_id)
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
            self.message_objects[x.id] = x
        state.update(status=OUTBOX, outbox_states=outbox_states)

    def apply_done(self, *, message_id: str, status: str):
        LOG.debug(f'apply_done {message_id} status={status}')
        if status not in (OK, ERROR, ERROR_NOTRY):
            raise InvalidStatusError(f'invalid done status {status}')
        state = self.state.get(message_id)
        if state is None:
            raise MessageNotExistsError(f"message {message_id} not exists")
        current_status = state['status']
        if current_status not in (EXECUTE, OUTBOX):
            raise ActorStateError(
                f"can not apply_done for message {message_id} status={current_status}")
        message = self.message_objects[message_id]
        outbox_states = state.pop('outbox_states', None)
        if outbox_states:
            for outbox_message_id in outbox_states:
                self.message_objects.pop(outbox_message_id, None)
        state.update(status=status, is_acked=False)
        self.done_message_ids[message.dst][message.src_node].add(message_id)
        if not message.require_ack:
            self.apply_complete(message_id=message_id)

    def apply_complete(self, *, message_id: str, status: str = None):
        LOG.debug(f'apply_complete {message_id} status={status}')
        if message_id in self.state:
            state = self.state[message_id]
            current_status = state['status']
            if current_status not in (INBOX, OK, ERROR, ERROR_NOTRY):
                raise ActorStateError(
                    f"can not apply_complete for message {message_id} status={current_status}")
            is_acked = state.get('is_acked')
            if is_acked:
                raise ActorStateError(f"message {message_id} already complete acked")
            message = self.message_objects[message_id]
            state.update(is_acked=True)
            if status is not None:
                state.update(status=status)
            self.done_message_ids[message.dst][message.src_node].discard(message_id)
            # TODO: do not pop when message is local and require_ack
            # self.message_objects.pop(message_id, None)
        else:
            if status not in (OK, ERROR, ERROR_NOTRY):
                raise InvalidStatusError(f'invalid complete status {status}')
            self.state[message_id] = dict(status=status, is_acked=True)

    def apply_export(self, *, outbox_message_id: str, retry_at: int = None):
        LOG.debug(f'apply_export outbox_message_id={outbox_message_id} retry_at={retry_at}')
        if outbox_message_id not in self.message_objects:
            raise ActorStateError(f'outbox_message {outbox_message_id} not exists')
        outbox_message = self.message_objects[outbox_message_id]
        message_id = outbox_message.parent_id
        state = self.state.get(message_id)
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
        outbox_state.update(status=EXPORT)
        if outbox_message.require_ack:
            outbox_state.update(retry_at=retry_at)
        else:
            self.apply_acked(outbox_message_id=outbox_message.id, status=OK)

    def apply_acked(self, *, outbox_message_id: str, status: str):
        LOG.debug(f'apply_acked outbox_message_id={outbox_message_id} status={status}')
        if outbox_message_id not in self.message_objects:
            raise ActorStateError(f'outbox_message {outbox_message_id} invalid or already acked')
        outbox_message = self.message_objects[outbox_message_id]
        if status not in (OK, ERROR, ERROR_NOTRY):
            raise InvalidStatusError(f'invalid acked status {status}')
        message_id = outbox_message.parent_id
        state = self.state.get(message_id)
        if state is None:
            raise MessageNotExistsError(f'invalid or already acked message {message_id}')
        current_status = state['status']
        if current_status != OUTBOX:
            raise ActorStateError(
                f"can not apply_acked for message {message_id} status={current_status}")
        outbox_states = state['outbox_states']
        outbox_state = outbox_states[outbox_message.id]
        outbox_current_status = outbox_state['status']
        if outbox_current_status != EXPORT:
            raise ActorStateError(
                f"can not apply_acked for outbox_message {outbox_message.id} "
                f"status={outbox_current_status}")
        outbox_state.update(status=status, retry_at=None)
        if status == ERROR_NOTRY:
            self.apply_done(message_id=message_id, status=ERROR_NOTRY)
        elif status == OK:
            all_ok = all(x['status'] == OK for x in outbox_states.values())
            if all_ok:
                self.apply_done(message_id=message_id, status=OK)
            else:
                outbox_states[outbox_message.id] = dict(status=status)
                self.message_objects[outbox_message.id] = outbox_message.meta()

    def apply_retry(self, *, outbox_message_id: str, executed_count: int = None):
        LOG.debug(
            f'apply_retry outbox_message_id={outbox_message_id} executed_count={executed_count}')
        if outbox_message_id not in self.message_objects:
            raise ActorStateError(f'outbox_message {outbox_message_id} not exists')
        outbox_message = self.message_objects[outbox_message_id]
        message_id = outbox_message.parent_id
        state = self.state.get(message_id)
        if state is None:
            raise MessageNotExistsError(f'invalid or already acked message {message_id}')
        current_status = state['status']
        if current_status != OUTBOX:
            raise ActorStateError(
                f"can not apply_retry for message {message_id} status={current_status}")
        outbox_states = state['outbox_states']
        outbox_state = outbox_states[outbox_message.id]
        outbox_current_status = outbox_state['status']
        if executed_count is None:
            if outbox_current_status not in (ERROR, EXPORT):
                raise ActorStateError(
                    f"can not apply_retry for outbox_message {outbox_message.id} "
                    f"status={outbox_current_status}")
            executed_count = outbox_state['executed_count'] + 1
        else:
            if outbox_current_status != OUTBOX:
                raise ActorStateError(
                    f"can not apply_retry for outbox_message {outbox_message.id} "
                    f"status={outbox_current_status} with executed_count={executed_count}")
        outbox_state.update(status=OUTBOX, executed_count=executed_count, retry_at=None)

    def apply_restart(self):
        LOG.debug(f'apply_restart')
        message_ids = []
        for message_id, state in self.state.items():
            if state['status'] == EXECUTE:
                message_ids.append(message_id)
        for message_id in message_ids:
            self.apply_done(message_id=message_id, status=ERROR)

    def _dump_outbox_state(self, outbox_message_id, outbox_state):
        outbox_message = self.message_objects[outbox_message_id]
        outbox_status = outbox_state['status']
        executed_count = outbox_state.get('executed_count')
        if executed_count and executed_count > 0:
            yield dict(type='retry', outbox_message_id=outbox_message_id, executed_count=executed_count)
        if outbox_status == EXPORT:
            retry_at = outbox_state.get('retry_at')
            yield dict(type='export', outbox_message_id=outbox_message_id, retry_at=retry_at)
        elif outbox_status == OK:
            yield dict(type='export', outbox_message_id=outbox_message_id)
            if outbox_message.require_ack:
                yield dict(type='acked', outbox_message_id=outbox_message_id, status=outbox_status)
        elif outbox_status == ERROR:
            yield dict(type='export', outbox_message_id=outbox_message_id)
            yield dict(type='acked', outbox_message_id=outbox_message_id, status=outbox_status)

    def dump(self):
        for message_id, state in self.state.items():
            status = state['status']
            if status in (OK, ERROR, ERROR_NOTRY):
                is_acked = state.get('is_acked')
                if is_acked:
                    yield dict(type='complete', message_id=message_id, status=status)
                else:
                    yield dict(type='done', message_id=message_id, status=status)
            elif status == INBOX:
                message = self.message_objects[message_id]
                yield dict(type='inbox', message=message)
            elif status == EXECUTE:
                message = self.message_objects[message_id]
                yield dict(type='inbox', message=message)
                yield dict(type='execute', message_id=message_id)
            elif status == OUTBOX:
                message = self.message_objects[message_id]
                yield dict(type='inbox', message=message)
                yield dict(type='execute', message_id=message_id)
                outbox_states = state['outbox_states']
                outbox_messages = []
                for outbox_message_id in outbox_states.keys():
                    outbox_message = self.message_objects[outbox_message_id]
                    outbox_messages.append(outbox_message)
                yield dict(type='outbox', message_id=message_id, outbox_messages=outbox_messages)
                for outbox_message_id, outbox_state in outbox_states.items():
                    yield from self._dump_outbox_state(outbox_message_id, outbox_state)
        for dst, state in self.upstream.items():
            for src_node in state:
                yield dict(type='notify', dst=dst, src_node=src_node, available=True)
