from typing import List
import logging
import os.path
from threading import RLock

import msgpack

from .state import ActorState, ActorStateError


LOG = logging.getLogger(__name__)


class ActorStorageBase:

    def __init__(self, max_pending_size=10**3, max_done_size=10**6):
        self._max_pending_size = max_pending_size
        self._max_done_size = max_done_size
        self._state = ActorState(
            max_pending_size=self._max_pending_size,
            max_done_size=self._max_done_size,
        )
        self._lock = RLock()

    @property
    def max_pending_size(self):
        return self._max_pending_size

    @property
    def max_done_size(self):
        return self._max_done_size

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        pass

    def __repr__(self):
        name = type(self).__name__
        return (
            f'<{name} begin={self.num_begin_messages} '
            f'send={self.num_send_messages} done={self.num_done_messages}>'
        )

    @property
    def num_begin_messages(self):
        return self._state.num_begin_messages

    @property
    def num_send_messages(self):
        return self._state.num_send_messages

    @property
    def num_pending_messages(self):
        return self._state.num_pending_messages

    @property
    def num_done_messages(self):
        return self._state.num_done_messages

    @property
    def num_messages(self):
        return self._state.num_messages

    @property
    def current_wal_size(self):
        return 0

    def should_compact(self):
        return False

    def compact(self):
        """do nothing"""

    def op(self, type, **kwargs):
        with self._lock:
            return self._op(type, **kwargs)

    def op_begin(self, message_id, **kwargs):
        with self._lock:
            return self._op('begin', message_id=message_id, **kwargs)

    def op_done(self, message_id, status):
        with self._lock:
            return self._op('done', message_id=message_id, status=status)

    def op_send(self, message_id, send_messages: List[dict]):
        with self._lock:
            return self._op('send', message_id=message_id, send_messages=send_messages)

    def op_ack(self, message_id, status):
        with self._lock:
            return self._op('ack', message_id=message_id, status=status)

    def op_retry(self, message_id):
        with self._lock:
            return self._op('retry', message_id=message_id)

    def op_restart(self):
        with self._lock:
            return self._op('restart')

    def dump(self):
        with self._lock:
            return self._state.dump()

    def get_message_state(self, message_id):
        with self._lock:
            return self._state.get_message_state(message_id)

    def query_send_messages(self):
        with self._lock:
            return self._state.query_send_messages()


class ActorMemoryStorage(ActorStorageBase):

    def _op(self, type, **kwargs):
        return self._state.apply(type, **kwargs)


class ActorLocalStorage(ActorStorageBase):

    def __init__(
        self,
        dir_path,
        max_pending_size=10**3,
        max_done_size=10**6,
        buffer_size=100 * 1024 * 1024,
        skip_invalid=False,
    ):
        super().__init__(max_pending_size=max_pending_size, max_done_size=max_done_size)
        self.buffer_size = buffer_size
        self.skip_invalid = skip_invalid
        dir_path = os.path.abspath(os.path.expanduser(dir_path))
        LOG.info(f'use local storage at {dir_path}')
        os.makedirs(dir_path, exist_ok=True)
        self.dir_path = dir_path
        self.compact_filepath = os.path.join(dir_path, 'z.msgpack')
        filepaths = self._load_filepaths(dir_path)
        has_data = bool(filepaths)
        if not filepaths:
            filepaths.append(os.path.join(dir_path, '0.msgpack'))
        self._filepaths = filepaths
        self._current_filepath = filepaths[-1]
        self._current = open(self._current_filepath, 'ab')
        self._packer = msgpack.Packer(use_bin_type=True)
        self._is_compacting = False
        self._current_wal_size = 0
        if has_data:
            self._current_wal_size = self._load_wal(self._state, filepaths)
            self.op_restart()

    @property
    def current_wal_size(self):
        return self._current_wal_size

    @property
    def current_filepath(self):
        return self._current_filepath

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
        filenames = [x for x in os.listdir(dir_path)]
        filepaths = []
        for x in sorted(filenames):
            p = os.path.join(dir_path, x)
            if p.lower() != self.compact_filepath:
                filepaths.append(p)
        return filepaths

    def _load_wal(self, state, filepaths: list):
        n = 0
        for item in self._load_msgpack_items(filepaths):
            try:
                state.apply(**item)
            except ActorStateError as ex:
                LOG.info(ex)
            except (KeyError, ValueError, AssertionError) as ex:
                if self.skip_invalid:
                    LOG.exception(ex)
                else:
                    raise
            else:
                n += 1
        return n

    def _load_msgpack_items(self, filepaths):
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

    def _get_expect_wal_size(self):
        return self.num_begin_messages + self.num_done_messages + \
            self.num_send_messages * 5

    def should_compact(self):
        with self._lock:
            if self._is_compacting:
                return False
            expect_size = self._get_expect_wal_size()
            return self._current_wal_size > 2 * expect_size

    def compact(self):
        with self._lock:
            if self._is_compacting:
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
            expect_size = self._get_expect_wal_size()
            LOG.info(
                f'compact begin current_wal_size={self._current_wal_size} '
                f'expect_wal_size={expect_size} '
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
        try:
            # do compact
            tmp_state = ActorState(
                max_pending_size=self._max_pending_size,
                max_done_size=self._max_done_size,
            )
            self._load_wal(tmp_state, filepaths)
            num_wal_items = 0
            with open(self.compact_filepath, 'wb') as f:
                for item in tmp_state.dump():
                    self._append_file(f, item)
                    num_wal_items += 1
        except Exception:
            os.remove(self.compact_filepath)
            raise
        try:
            for filepath in filepaths:
                os.remove(filepath)
            os.rename(self.compact_filepath, prev_filepath)
        except Exception:
            # TODO: data lost
            self._filepaths = [self._current_filepath]
            raise
        else:
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

    def _op(self, type, **kwargs):
        ret = self._state.apply(type, **kwargs)
        self._append_file(self._current, {'type': type, **kwargs})
        self._current_wal_size += 1
        return ret
