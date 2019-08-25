import tempfile

from actorlib.state import ActorState
from actorlib.storage import ActorLocalStorage, ActorMemoryStorage


def op(op_type, message_id, **kwargs):
    return dict(type=op_type, message_id=message_id, **kwargs)


def make_wal_items(msg_id, status=None):
    msg_s1 = f'{msg_id}_S1'
    msg_s2 = f'{msg_id}_S2'
    items = [
        op('begin', msg_id, src='src', src_node='src_node', dst='dst', is_ask=False),
        op('send', msg_id, send_messages=[dict(id=msg_s1), dict(id=msg_s2)]),
        op('ack', message_id=msg_s1, status='OK'),
        op('ack', message_id=msg_s2, status='ERROR'),
        op('retry', message_id=msg_s2),
        op('ack', message_id=msg_s2, status='ERROR'),
    ]
    if status is not None:
        items.append(op('done', msg_id, status=status))
    return items


def load_wal(state, wal_items):
    for item in wal_items:
        state.apply(**item)


def test_actor_state():
    state = ActorState()
    wal_items = [
        op('begin', 'MSG_ID', src='src', src_node='src_node', dst='dst', is_ask=False),
        op('send', 'MSG_ID', send_messages=[dict(id='S1'), dict(id='S2')]),
        op('ack', message_id='S1', status='OK'),
        op('ack', message_id='S2', status='ERROR'),
        op('retry', message_id='S2'),
    ]
    load_wal(state, wal_items)
    assert set(state.query_send_messages()) == {'S2'}
    msg = state.get_message_state('MSG_ID')
    assert msg['status'] == 'SEND'
    assert msg['send_messages']['S1']['status'] == 'OK'
    assert msg['send_messages']['S2']['status'] == 'ERROR'
    assert msg['send_messages']['S2']['count'] == 1
    state.apply(**op('ack', message_id='S2', status='OK'))
    msg = state.get_message_state('MSG_ID')
    assert msg['status'] == 'OK'
    assert set(state.query_send_messages()) == set()
    assert list(state.dump()) == [
        op('done', 'MSG_ID', status='OK')
    ]


def test_actor_state_compact():
    state = ActorState(max_pending_size=100, max_done_size=50)
    for i in range(100):
        for status in ['OK', None, 'ERROR']:
            msg_id = f'{i}_{status}'
            load_wal(state, make_wal_items(msg_id, status=status))
    assert state.num_done_messages == 50
    assert state.num_begin_messages == 0
    assert state.num_send_messages == 100
    assert state.num_messages == 150
    wal_items = list(state.dump())
    assert len(wal_items) == 550


def test_actor_local_storage():
    with tempfile.TemporaryDirectory() as dir_path:
        # test write data
        with ActorLocalStorage(dir_path, max_pending_size=100, max_done_size=50) as storage:
            for i in range(100):
                for status in ['OK', None, 'ERROR']:
                    msg_id = f'{i}_{status}'
                    for item in make_wal_items(msg_id, status=status):
                        storage.op(**item)
            assert storage.current_wal_size == 6 * 100 + 7 * 2 * 100
            assert storage.should_compact()
        # test read from disk and compact
        with ActorLocalStorage(dir_path, max_pending_size=100, max_done_size=50) as storage:
            assert storage.should_compact()
            storage.compact()
            assert not storage.should_compact()
            assert storage.current_wal_size == 550
        # test read from disk after compact
        with ActorLocalStorage(dir_path, max_pending_size=100, max_done_size=50) as storage:
            assert not storage.should_compact()
            assert storage.current_wal_size == 551


def test_actor_memory_storage():
    with ActorMemoryStorage() as storage:
        for i in range(100):
            for status in ['OK', None, 'ERROR']:
                msg_id = f'{i}_{status}'
                for item in make_wal_items(msg_id, status=status):
                    storage.op(**item)
        assert len(set(storage.query_send_messages())) == 100
