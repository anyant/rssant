from typing import List, Dict, Tuple, Optional
import logging
from collections import OrderedDict

from .message import ActorMessage


LOG = logging.getLogger(__name__)


class ActorStateError(Exception):
    """ActorStateError"""


class DuplicateMessageError(ActorStateError):
    """Duplicate message error"""


class MessageNotExistsError(ActorStateError):
    """Message not in state or ack/retry message not in send messages"""


class InvalidStatusError(ActorStateError):
    """Invalid message status or ack status"""


class TooManyMessagesError(ActorStateError):
    """Too many messages error"""


BEGIN = "BEGIN"
SEND = "SEND"
OK = "OK"
ERROR = "ERROR"
ERROR_NOTRY = "ERROR_NOTRY"

MESSAGE_STATUS = (BEGIN, SEND, OK, ERROR, ERROR_NOTRY)
SEND_MESSAGE_STATUS = (BEGIN, OK, ERROR, ERROR_NOTRY)


class ActorState:
    """
    WAL + memory state
    """

    def __init__(self, max_pending_size=10**3, max_done_size=10**6):
        """
        """
        if max_pending_size is None or max_pending_size <= 0:
            raise ValueError(f'invalid max_pending_size {max_pending_size}')
        self.max_pending_size = max_pending_size
        if max_done_size is None or max_done_size <= 0:
            raise ValueError(f'invalid max_done_size {max_done_size}')
        self.max_done_size = max_done_size
        # state:
        # {
        #     MESSAGE_ID: {
        #         "status": MESSAGE_STATUS,
        #         "message": {
        #             "require_ack": REQUIRE_ACK,
        #             "dst": DST,
        #             "src": SRC,
        #             "src_node": SRC_NODE,
        #         },
        #         "send_messages": {
        #             SEND_MESSAGE_ID: {
        #                 "status": SEND_MESSAGE_STATUS,
        #                 "count": SEND_MESSAGE_RETRY_COUNT,
        #             }
        #         }
        #     }
        # }
        self._state = OrderedDict()
        # send_messages: {
        #     SEND_MESSAGE_ID: (PARENT_ID, SEND_MESSAGE)
        # }
        self._send_messages = OrderedDict()
        self._num_begin_messages = 0
        self._num_send_messages = 0
        self._num_done_messages = 0

    @property
    def num_begin_messages(self):
        return self._num_begin_messages

    @property
    def num_send_messages(self):
        return self._num_send_messages

    @property
    def num_pending_messages(self):
        return self.num_begin_messages + self.num_send_messages

    @property
    def num_done_messages(self):
        return self._num_done_messages

    @property
    def num_messages(self):
        return len(self._state)

    def __repr__(self):
        name = type(self).__name__
        return (
            f'<{name} begin={self.num_begin_messages} '
            f'send={self.num_send_messages} done={self.num_done_messages}>'
        )

    def apply_begin(self, message_id, *, require_ack, dst, src, src_node) -> None:
        LOG.info(
            f'apply_begin {message_id} require_ack={require_ack} dst={dst} '
            f'src={src} src_node={src_node}')
        state = self._state.get(message_id)
        if state is not None:
            status = state['status']
            if status != ERROR and status != ERROR_NOTRY:
                raise DuplicateMessageError(
                    f'duplicate message {message_id} status={status}')
        self._check_pending_messages()
        if state is not None:
            self._num_done_messages -= 1
        message = dict(require_ack=require_ack, dst=dst, src=src, src_node=src_node)
        self._state[message_id] = dict(status=BEGIN, message=message)
        self._num_begin_messages += 1
        self._check_num_messages()

    def apply_done(self, message_id, status) -> Dict:
        LOG.info(f'apply_done {message_id} status={status}')
        if status not in (OK, ERROR, ERROR_NOTRY):
            raise InvalidStatusError(f'invalid done status {status}')
        if message_id in self._state:
            state = self._state[message_id]
            current_status = state['status']
            if current_status not in (BEGIN, SEND):
                raise ActorStateError(
                    f"message {message_id} already done status={current_status}")
            if current_status == BEGIN:
                self._num_begin_messages -= 1
            else:  # == SEND
                self._num_send_messages -= 1
        else:
            state = self._state.setdefault(message_id, {})
        state.update(status=status)
        send_messages = state.pop('send_messages', None)
        if send_messages:
            for send_msg_id in send_messages:
                self._send_messages.pop(send_msg_id, None)
        self._num_done_messages += 1
        self._check_num_messages()
        ret = dict(id=message_id, status=status, **state.get('message', {}))
        self._compact_done_messages()
        return ret

    @staticmethod
    def _format_send_messages(send_messages):
        text = []
        for x in send_messages:
            try:
                text.append(repr(ActorMessage.from_dict(x)))
            except KeyError:
                text.append(x['id'])
        return ','.join(text)

    def apply_send(self, message_id, send_messages: List[Dict]) -> None:
        send_messages_text = self._format_send_messages(send_messages)
        LOG.info(f'apply_send {message_id} send_messages={send_messages_text}')
        if not send_messages:
            raise ActorStateError("can not send empty messages")
        state = self._state.get(message_id, None)
        if state is None:
            raise MessageNotExistsError(f'begin not apply for message {message_id}')
        current_status = state['status']
        if current_status != BEGIN:
            raise ActorStateError(f"can not send messages in {current_status} status")
        send_messages_state = {}
        for x in send_messages:
            self._send_messages[x['id']] = (message_id, x)
            send_messages_state[x['id']] = dict(status=BEGIN, count=0)
        state.update(status=SEND, send_messages=send_messages_state)
        self._num_begin_messages -= 1
        self._num_send_messages += 1
        self._check_num_messages()

    def apply_ack(self, message_id, status) -> Optional[Dict]:
        LOG.info(f'apply_ack {message_id} status={status}')
        if status not in (OK, ERROR, ERROR_NOTRY):
            raise InvalidStatusError(f'invalid ack status {status}')
        if message_id not in self._send_messages:
            raise MessageNotExistsError(f'invalid or already acked message {message_id}')
        parent_id, __ = self._send_messages[message_id]
        self._check_parent_send_state(parent_id)
        send_messages = self._state[parent_id]['send_messages']
        send_messages[message_id].update(status=status)
        if status == ERROR_NOTRY:
            return self.apply_done(parent_id, ERROR_NOTRY)
        elif status == OK:
            all_ok = all(x['status'] == OK for x in send_messages.values())
            if all_ok:
                return self.apply_done(parent_id, OK)
            else:
                self._send_messages.pop(message_id, None)
        self._check_num_messages()
        return None

    def apply_retry(self, message_id) -> None:
        LOG.info(f'apply_ack {message_id}')
        if message_id not in self._send_messages:
            raise MessageNotExistsError(f'invalid or already acked message {message_id}')
        parent_id, __ = self._send_messages[message_id]
        self._check_parent_send_state(parent_id)
        message_state = self._state[parent_id]['send_messages'][message_id]
        message_state.update(count=message_state['count'] + 1)
        self._check_num_messages()

    def apply_restart(self) -> None:
        LOG.info(f'apply_restart {self}')
        msg_ids = []
        for msg_id, state in self._state.items():
            if state['status'] == BEGIN:
                msg_ids.append(msg_id)
        for msg_id in msg_ids:
            del self._state[msg_id]
        self._num_begin_messages = 0
        self._check_num_messages()

    def apply(self, type, **kwargs):
        return getattr(self, 'apply_' + type)(**kwargs)

    def _check_num_messages(self):
        total = sum([
            self.num_begin_messages,
            self.num_send_messages,
            self.num_done_messages,
        ])
        assert total == self.num_messages, 'num_messages not correct, bug!'

    def _check_parent_send_state(self, parent_id):
        assert parent_id in self._state, 'parent_id not in self._state, bug!'
        parent_status = self._state[parent_id]['status']
        assert parent_status == SEND, 'parent_status is not SEND, bug!'

    @staticmethod
    def _is_done(status):
        return status in (OK, ERROR, ERROR_NOTRY)

    def _check_pending_messages(self):
        if self.num_pending_messages > self.max_pending_size:
            raise TooManyMessagesError(
                f'too many pending messages: num_begin={self.num_begin_messages} '
                f'num_send={self.num_send_messages}')

    def _compact_done_messages(self):
        skip_cnt = self.num_done_messages - self.max_done_size
        if skip_cnt <= 0:
            return
        done_msg_ids = []
        for msg_id, state in self._state.items():
            if self._is_done(state['status']):
                done_msg_ids.append(msg_id)
                skip_cnt -= 1
                if skip_cnt <= 0:
                    break
        for msg_id in done_msg_ids:
            del self._state[msg_id]
            self._num_done_messages -= 1
        self._check_num_messages()

    def dump(self):
        for message_id, state in self._state.items():
            status = state['status']
            if self._is_done(status):
                yield dict(type='done', message_id=message_id, status=status)
            elif status == BEGIN:
                yield dict(type='begin', message_id=message_id, **state['message'])
            elif status == SEND:
                yield dict(type='begin', message_id=message_id, **state['message'])
                send_messages = []
                for k, ack_state in state['send_messages'].items():
                    if k in self._send_messages:
                        __, msg = self._send_messages[k]
                    else:
                        msg = {'id': k}
                    send_messages.append(msg)
                yield dict(type='send', message_id=message_id, send_messages=send_messages)
                for ack_message_id, ack_state in state['send_messages'].items():
                    ack_status = ack_state['status']
                    count = ack_state['count']
                    for i in range(count):
                        yield dict(type='retry', message_id=ack_message_id)
                    if self._is_done(ack_status):
                        yield dict(type='ack', message_id=ack_message_id, status=ack_status)

    def get_message_state(self, message_id) -> dict:
        state = self._state.get(message_id, None)
        if state is None:
            raise MessageNotExistsError(f'message {message_id} not exists')
        return dict(id=message_id, **state)

    def query_send_messages(self) -> List[Dict[str, Tuple[dict, dict]]]:
        ret = {}
        for msg_id, (parent_id, msg) in self._send_messages.items():
            self._check_parent_send_state(parent_id)
            state = self._state[parent_id]['send_messages'][msg_id]
            ret[msg_id] = (state, msg)
        return ret
