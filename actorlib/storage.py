import logging
import os.path
from collections import namedtuple

import msgpack

from .message import ActorMessage
from .state import ActorState, ActorStateError


LOG = logging.getLogger(__name__)


CompactPrepareInfo = namedtuple('CompactPrepareInfo', 'current_filepath, wal_items')

OP_INBOX = 'inbox'
OP_OUTBOX = 'outbox'


class ActorLocalStorage:
    def __init__(self, dirpath: str, compact_wal_delta: int = 1000):
        dirpath = os.path.abspath(os.path.expanduser(dirpath))
        LOG.info(f'use local storage at {dirpath}')
        os.makedirs(dirpath, exist_ok=True)
        self.dirpath = dirpath
        self.compact_filename = 'z.msgpack'
        self.compact_filepath = os.path.join(dirpath, self.compact_filename)
        filepaths = self._load_filepaths(dirpath)
        if not filepaths:
            filepaths.append(os.path.join(dirpath, '0.msgpack'))
        self.filepaths = filepaths
        self.current_filepath = filepaths[-1]
        self.compact_wal_delta = compact_wal_delta
        self.non_current_wal_size = 0
        self.current_storage = ActorLocalStorageFile(filepath=self.current_filepath)
        self.is_compacting = False

    @property
    def wal_size(self):
        return self.non_current_wal_size + self.current_storage.wal_size

    def close(self):
        self.current_storage.close()

    def load(self, state: ActorState):
        for filepath in self.filepaths[:-1]:
            LOG.info(f'load storage file {filepath}')
            with ActorLocalStorageFile(filepath=filepath) as storage:
                storage.load(state)
                self.non_current_wal_size += storage.wal_size
        self.current_storage.load(state)

    def append(self, type, **kwargs):
        self.current_storage.append(type=type, **kwargs)

    def should_compact(self, state: ActorState):
        if self.is_compacting:
            return False
        if self.wal_size < self.compact_wal_delta // 10:
            return False
        state_wal_size = state.wal_size
        if self.wal_size > state_wal_size * 3:
            return True
        if self.wal_size - state_wal_size > self.compact_wal_delta:
            return True
        return False

    def prepare_compact(self, state: ActorState) -> CompactPrepareInfo:
        if not self.should_compact(state):
            return None
        self.is_compacting = True
        LOG.info(
            f'compact begin wal_size={self.wal_size} state_wal_size={state.wal_size} '
            f'current_filepath={self.current_filepath}'
        )
        # switch to next storage
        current_filepath = self.current_filepath
        current_storage = self.current_storage
        next_filepath = self.get_next_filepath()
        next_storage = ActorLocalStorageFile(filepath=next_filepath)
        self.filepaths.append(next_filepath)
        self.current_filepath = next_filepath
        self.current_storage = next_storage
        self.non_current_wal_size += current_storage.wal_size
        current_storage.close()
        wal_items = list(state.dump())
        return CompactPrepareInfo(current_filepath, wal_items)

    def _state_from_wal(self, wal_items: list):
        state = ActorState()
        for item in wal_items:
            try:
                state.apply(**item)
            except ActorStateError as ex:
                LOG.warning(ex)
        return state

    def compact(self, prepare_info: CompactPrepareInfo):
        """
        Do compact, make sure call prepare_compact before compact.
        No need lock in this method.
        """
        if not prepare_info:
            return
        current_filepath, wal_items = prepare_info
        state = self._state_from_wal(wal_items)
        try:
            # save current state to compact storage
            try:
                with ActorLocalStorageFile(filepath=self.compact_filepath) as storage:
                    storage.save(state)
                    self.non_current_wal_size = storage.wal_size
            except Exception:
                os.remove(self.compact_filepath)
                raise
            # remove old storages, rename compact file
            try:
                for filepath in self.filepaths[:-1]:
                    os.remove(filepath)
                os.rename(self.compact_filepath, current_filepath)
            except Exception:
                # TODO: data lost
                self.filepaths = [self.current_filepath]
                raise
        except Exception:
            self.is_compacting = False
            raise
        # finish compact
        self.filepaths = [current_filepath, self.current_filepath]
        self.is_compacting = False
        LOG.info(
            f'compact end wal_size={self.wal_size} '
            f'current_filepath={self.current_filepath}'
        )

    def _get_file_num(self, filepath):
        num, _ = os.path.splitext(os.path.basename(filepath))
        return int(num)

    def get_next_filepath(self):
        next_file_num = self._get_file_num(self.current_filepath) + 1
        return os.path.join(self.dirpath, f'{next_file_num}.msgpack')

    def _load_filepaths(self, dirpath):
        filenames = [x for x in os.listdir(dirpath)]
        filepaths = []
        for x in sorted(filenames):
            if x.lower() == self.compact_filename:
                continue
            filepath = os.path.join(dirpath, x)
            if os.path.isfile(filepath):
                filepaths.append(filepath)
        return filepaths


class ActorLocalStorageFile:
    def __init__(self, fileobj=None, filepath=None):
        if all([fileobj is None, filepath is None]):
            raise ValueError('require fileobj or filepath')
        self._filepath = filepath
        self._fileobj = fileobj
        self.packer = msgpack.Packer(use_bin_type=True)
        self.unpacker_buffer_size = 16 * 1024 * 1024
        self.buffer_size = 16 * 1024
        self.wal_size = 0

    def __repr__(self):
        name = type(self).__name__
        return f'<{name} {self._fileobj} wal_size={self.wal_size}>'

    @property
    def fileobj(self):
        return self.__enter__()._fileobj

    def __enter__(self):
        if self._fileobj is None:
            self._fileobj = open(self._filepath, 'ab+')
        return self

    def __exit__(self, *exc_info):
        if self._fileobj is not None:
            self._fileobj.close()

    def create_unpacker(self):
        unpacker = msgpack.Unpacker(raw=False, max_buffer_size=self.unpacker_buffer_size)
        return MsgpackUnpackerWrapper(unpacker)

    def close(self):
        self._fileobj.close()

    def load(self, state: ActorState):
        self.fileobj.seek(0)
        unpacker = self.create_unpacker()
        while True:
            buf = self.fileobj.read(self.buffer_size)
            if not buf:
                break
            unpacker.feed(buf)
            try:
                for item in unpacker:
                    item = self._message_from_dict(**item)
                    try:
                        state.apply(**item)
                    except ActorStateError as ex:
                        LOG.warning(ex)
                    self.wal_size += 1
            except DirtyMsgpackFile as ex:
                LOG.error('dirty msgpack file, will lost some data!', exc_info=ex)
                self.fileobj.seek(unpacker.tell())
                break

    def save(self, state: ActorState):
        for item in state.dump():
            self.append(**item)

    def _message_from_dict(self, type, **kwargs):
        if type == OP_INBOX:
            kwargs['message'] = ActorMessage.from_dict(kwargs['message'])
        elif type == OP_OUTBOX:
            messages = [ActorMessage.from_dict(x) for x in kwargs['outbox_messages']]
            kwargs['outbox_messages'] = messages
        return {'type': type, **kwargs}

    def _message_to_dict(self, type, **kwargs):
        if type == OP_INBOX:
            kwargs['message'] = kwargs['message'].to_complete_dict()
        elif type == OP_OUTBOX:
            kwargs['outbox_messages'] = [x.to_complete_dict() for x in kwargs['outbox_messages']]
        return {'type': type, **kwargs}

    def append(self, type, **kwargs):
        item = self._message_to_dict(type=type, **kwargs)
        self.fileobj.write(self.packer.pack(item))
        self.wal_size += 1


class DirtyMsgpackFile(Exception):
    """Dirty msgpack file"""


class MsgpackUnpackerWrapper:
    def __init__(self, unpacker):
        self._unpacker = unpacker

    def __getattr__(self, *args, **kwargs):
        return getattr(self._unpacker, *args, **kwargs)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            item = self._unpacker.__next__()
        except UnicodeDecodeError as ex:
            raise DirtyMsgpackFile(f'dirty msgpack file {ex}') from ex
        else:
            if not isinstance(item, dict):
                raise DirtyMsgpackFile(f'dirty msgpack item type {type(item)}')
            return item

    next = __next__
