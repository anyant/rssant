from typing import List
import logging
import os.path
from threading import RLock

import msgpack


LOG = logging.getLogger(__name__)


class ActorStateError(Exception):
    """ActorStateError"""


class DuplicateMessageError(ActorStateError):
    """DuplicateMessageError"""


class ActorState:
    """
    WAL + memory state

    begin -> send -> ack1 -> ack2 ... -> done_OK
        |                      |-> retry -> ERROR -> done_ERROR
        |-> done_ERROR

    op_begin MSG_ID
    op_send MSG_ID SENDING_MESSAGES[1,2,3]
    op_ack ACK_MSG_ID_1 OK
    op_ack ACK_MSG_ID_2 OK
    op_ack ACK_MSG_ID 3 OK
    # op_done MSG_ID OK

    op_begin MSG_ID
    op_send MSG_ID SENDING_MESSAGES[1,2,3]
    op_ack ACK_MSG_ID_1 OK
    op_ack ACK_MSG_ID_2 ERR
    op_retry ACK_MSG_ID_2
    op_retry ACK_MSG_ID_3
    op_ack ACK_MSG_ID_2 OK
    op_ack ACK_MSG_ID_3 OK
    # op_done MSG_ID OK

    op_begin MSG_ID
    op_done MSG_ID ERR

    op_begin MSG_ID
    op_send MSG_ID SENDING_MESSAGES[1,2,3]
    op_ack ACK_MSG_ID_1 OK
    op_ack ACK_MSG_ID_2 ERR
    op_retry ACK_MSG_ID_2
    op_retry ACK_MSG_ID_3
    op_retry ACK_MSG_ID_2
    op_retry ACK_MSG_ID_3
    op_done MSG_ID ERR

    op_done MSG_ID OK/ERR


    status: BEGIN/SEND/OK/ERROR/ERROR_NOTRY
    """
    # TODO: compact done message in state

    def __init__(self):
        # state:
        # {
        #     MESSAGE_ID: {
        #         "status": STATUS,
        #         "src_node": SRC_NODE,
        #         "send_messages": {
        #             MESSAGE_ID: {
        #                 "status": STATUS,
        #                 "count": COUNT,
        #             }
        #         }
        #     }
        # }
        self._state = {}
        # send_messages: { MESSAGE_ID: (PARENT_ID, MESSAGE) }
        self._send_messages = {}
        self._done_message_ids = []

    def apply_begin(self, message_id, src_node):
        state = self._state.get(message_id)
        if state is not None:
            status = state['status']
            if status != 'ERROR' and status != 'ERROR_NOTRY':
                self._done_message_ids.append(message_id)
                raise DuplicateMessageError(f'duplicate message {message_id} status={status}')
        self._state[message_id] = dict(status='BEGIN', src_node=src_node)

    def apply_done(self, message_id, status):
        self._state.setdefault(message_id, {}).update(status=status)
        send_messages = self._state[message_id].pop('send_messages', None)
        if send_messages:
            for send_msg_id in send_messages:
                self._send_messages.pop(send_msg_id, None)
        self._done_message_ids.append(message_id)

    def apply_send(self, message_id, send_messages: List[dict]):
        send_messages_state = {}
        for x in send_messages:
            self._send_messages[x['id']] = (message_id, x)
            send_messages_state[x['id']] = dict(status='BEGIN', count=0)
        self._state[message_id].update(status='SEND', send_messages=send_messages_state)

    def apply_ack(self, message_id, status):
        parent_id, __ = self._send_messages[message_id]
        send_messages = self._state[parent_id]['send_messages']
        send_messages[message_id].update(status=status)
        if status == 'ERROR_NOTRY':
            self.apply_done(parent_id, 'ERROR_NOTRY')
        elif status == 'OK':
            all_ok = all(x['status'] == 'OK' for x in send_messages.values())
            if all_ok:
                self.apply_done(parent_id, 'OK')
            else:
                self._send_messages.pop(message_id, None)

    def apply_retry(self, message_id):
        parent_id, __ = self._send_messages[message_id]
        message_state = self._state[parent_id]['send_messages'][message_id]
        message_state.update(count=message_state['count'] + 1)

    def query_message_ids(self):
        return set(self._state.keys())

    def get_message_state(self, message_id):
        return self._state[message_id]

    def query_send_messages(self) -> [(str, dict, dict)]:
        ret = {}
        for msg_id, (parent_id, msg) in self._send_messages.items():
            state = self._state[parent_id]['send_messages'][msg_id]
            ret[msg_id] = (state, msg)
        return ret

    def pop_done_messages(self):
        ret = {}
        for msg_id in self._done_message_ids:
            state = self._state[msg_id]
            ret[msg_id] = dict(status=state['status'], src_node=state['src_node'])
        self._done_message_ids = []
        return ret

    def apply(self, item):
        item = item.copy()
        op_type = item.pop('type')
        getattr(self, 'apply_' + op_type)(**item)

    def load(self, wal_items: list):
        for item in wal_items:
            self.apply(item)
        return self

    @staticmethod
    def _is_done(x):
        return x == 'OK' or x == 'ERROR' or x == 'ERROR_NOTRY'

    def _dump(self):
        for message_id, state in self._state.items():
            status = state['status']
            if self._is_done(status):
                yield dict(type='done', message_id=message_id, status=status)
            elif status == 'BEGIN':
                yield dict(type='begin', message_id=message_id)
            elif status == 'SEND':
                yield dict(type='begin', message_id=message_id)
                send_messages = [self._send_messages[k] for k in state['send_messages']]
                yield dict(type='send', message_id=message_id, send_messages=send_messages)
                for ack_message_id, message_state in state['send_messages'].items():
                    ack_status = message_state['status']
                    count = message_state['count']
                    for i in range(count):
                        yield dict(type='retry', message_id=message_id,
                                   ack_message_id=ack_message_id)
                    if self._is_done(ack_status):
                        yield dict(type='ack', message_id=message_id,
                                   ack_message_id=ack_message_id, status=ack_status)
                    elif ack_status == 'BEGIN':
                        pass
                    else:
                        assert False, f'unknown ack_status {ack_status}'
            else:
                assert False, f'unknown status {status}'

    def dump(self):
        return list(self._dump())

    def compact(self, limit=None):
        wal_items = list(self._dump())
        if limit is None or limit >= len(wal_items):
            return wal_items
        ret = []
        skip_cnt = len(wal_items) - limit
        for i, item in enumerate(wal_items):
            if item['type'] == 'done':
                skip_cnt -= 1
                if skip_cnt <= 0:
                    break
            else:
                ret.append(item)
        ret.extend(wal_items[i + 1:])
        return ret


COMPACT_FILENAME = 'z.msgpack'


class ActorStorageBase:

    def __init__(self):
        self._state = ActorState()
        self._lock = RLock()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        with self._lock:
            self._state = None

    @property
    def current_wal_size(self):
        return 0

    def should_compact(self):
        return False

    def compact(self):
        """do nothing"""

    def op(self, item):
        with self._lock:
            self._op(**item)

    def op_begin(self, message_id):
        with self._lock:
            self._op('begin', message_id)

    def op_done(self, message_id, status):
        with self._lock:
            self._op('done', message_id, status=status)

    def op_send(self, message_id, send_messages: List[dict]):
        with self._lock:
            self._op('send', message_id, send_messages=send_messages)

    def op_ack(self, message_id, status):
        with self._lock:
            self._op('ack', message_id, status=status)

    def op_retry(self, message_id):
        with self._lock:
            self._op('retry', message_id)

    def query_send_messages(self):
        with self._lock:
            return self._state.query_send_messages()

    def pop_done_messages(self):
        with self._lock:
            return self._state.pop_done_messages()


class ActorMemoryStorage(ActorStorageBase):

    def _op(self, type, message_id, **kwargs):
        getattr(self._state, 'apply_' + type)(message_id, **kwargs)


class ActorLocalStorage(ActorStorageBase):

    def __init__(self, dir_path, wal_limit=10**6, buffer_size=100 * 1024 * 1024):
        super().__init__()
        self.dir_path = dir_path
        self.wal_limit = wal_limit
        self.buffer_size = buffer_size
        self._current_wal_size = 0
        LOG.info(f'use local storage at {dir_path}')
        filepaths = self._load_filepaths(dir_path)
        if filepaths:
            for item in self._load_items(filepaths):
                self._state.apply(item)
                self._current_wal_size += 1
        else:
            filepaths.append(os.path.join(dir_path, '0.msgpack'))
        self._filepaths = filepaths
        self._current_filepath = filepaths[-1]
        self._current = open(self._current_filepath, 'ab')
        self._packer = msgpack.Packer(use_bin_type=True)
        self._is_compacting = False

    @property
    def current_wal_size(self):
        with self._lock:
            return self._current_wal_size

    def close(self):
        with self._lock:
            super().close()
            self._current.close()

    def _get_file_num(self, filepath):
        num, _ = os.path.splitext(os.path.basename(filepath))
        return int(num)

    def _get_next_filepath(self):
        next_file_num = self._get_file_num(self._current_filepath) + 1
        return os.path.join(self.dir_path, f'{next_file_num}.msgpack')

    def _load_filepaths(self, dir_path):
        filenames = [x for x in os.listdir(dir_path) if x.lower() != COMPACT_FILENAME]
        filepaths = [os.path.join(dir_path, x) for x in sorted(filenames)]
        return filepaths

    def _load_items(self, filepaths):
        unpacker = msgpack.Unpacker(raw=False, max_buffer_size=self.buffer_size)
        for filepath in filepaths:
            with open(filepath, 'rb') as f:
                while True:
                    buf = f.read(64 * 1024)
                    if not buf:
                        break
                    unpacker.feed(buf)
                    for item in unpacker:
                        yield item

    def should_compact(self):
        with self._lock:
            if self._is_compacting:
                return False
            return self._current_wal_size > 3 * self.wal_limit

    def compact(self):
        with self._lock:
            if self._is_compacting:
                return
            if self._current_wal_size < self.wal_limit:
                return
            self._is_compacting = True
        try:
            self._do_compact()
        finally:
            with self._lock:
                self._is_compacting = False

    def _do_compact(self):
        # switch current file
        with self._lock:
            LOG.info(
                f'compact begin current_wal_size={self._current_wal_size} '
                f'wal_limit={self.wal_limit} '
                f'current_filepath={self._current_filepath}'
            )
            filepaths = self._filepaths.copy()
            next_filepath = self._get_next_filepath()
            prev_filepath = self._current_filepath
            prev_current_wal_size = self._current_wal_size
            self._filepaths.append(next_filepath)
            self._current_filepath = next_filepath
            self._current.close()
            self._current = open(self._current_filepath, 'ab')
        # do compact
        tmp_state = ActorState()
        tmp_state.load(self._load_items(filepaths))
        wal_items = tmp_state.compact(limit=self.wal_limit)
        num_wal_items = 0
        with open(COMPACT_FILENAME, 'wb') as f:
            for item in wal_items:
                self._append_file(f, item)
                num_wal_items += 1
        for filepath in filepaths:
            os.remove(filepath)
        os.rename(COMPACT_FILENAME, prev_filepath)
        # update some vars
        with self._lock:
            self._filepaths = [prev_filepath, self._current_filepath]
            delta = self._current_wal_size - prev_current_wal_size
            self._current_wal_size = num_wal_items + delta
            LOG.info(
                f'compact end current_wal_size={self._current_wal_size} '
                f'current_filepath={self._current_filepath}'
            )

    def _append_file(self, file, item):
        self._current.write(self._packer.pack(item))

    def _append(self, item):
        self._append_file(self._current, item)

    def _op(self, type, message_id, **kwargs):
        self._append({'type': type, 'message_id': message_id, **kwargs})
        self._current_wal_size += 1
        getattr(self._state, 'apply_' + type)(message_id, **kwargs)
