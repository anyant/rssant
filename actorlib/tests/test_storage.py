import tempfile
from actorlib.storage import ActorState, ActorLocalStorage, ActorMemoryStorage


def op(op_type, message_id, **kwargs):
    return dict(type=op_type, message_id=message_id, **kwargs)


def make_wal_items(msg_id, status=None):
    msg_s1 = f'{msg_id}_S1'
    msg_s2 = f'{msg_id}_S2'
    items = [
        op('begin', msg_id, src_node='src', is_ask=False),
        op('send', msg_id, send_messages=[dict(id=msg_s1), dict(id=msg_s2)]),
        op('ack', message_id=msg_s1, status='OK'),
        op('ack', message_id=msg_s2, status='ERROR'),
        op('retry', message_id=msg_s2),
        op('ack', message_id=msg_s2, status='ERROR'),
    ]
    if status is not None:
        items.append(op('done', msg_id, status=status))
    return items


def test_actor_state():
    state = ActorState()
    wal_items = [
        op('begin', 'MSG_ID', src_node='src', is_ask=False),
        op('send', 'MSG_ID', send_messages=[dict(id='S1'), dict(id='S2')]),
        op('ack', message_id='S1', status='OK'),
        op('ack', message_id='S2', status='ERROR'),
        op('retry', message_id='S2'),
    ]
    state.load(wal_items)
    assert set(state.query_send_messages()) == {'S2'}
    msg = state.get_message_state('MSG_ID')
    assert msg['status'] == 'SEND'
    assert msg['send_messages']['S1']['status'] == 'OK'
    assert msg['send_messages']['S2']['status'] == 'ERROR'
    assert msg['send_messages']['S2']['count'] == 1
    state.apply(op('ack', message_id='S2', status='OK'))
    assert msg['status'] == 'OK'
    assert set(state.query_send_messages()) == set()
    assert state.compact() == [
        op('done', 'MSG_ID', status='OK')
    ]


def test_actor_state_compact():
    state = ActorState()
    for i in range(100):
        for status in ['OK', None, 'ERROR']:
            msg_id = f'{i}_{status}'
            state.load(make_wal_items(msg_id, status=status))
    wal_items = state.dump()
    assert len(wal_items) == 700
    compact_wal_items = state.compact(limit=550)
    assert len(compact_wal_items) == 550
    compact_wal_items = state.compact(limit=200)
    assert len(compact_wal_items) == 500


def test_actor_local_storage():
    with tempfile.TemporaryDirectory() as dir_path:
        # test write data
        with ActorLocalStorage(dir_path, wal_limit=550) as storage:
            for i in range(100):
                for status in ['OK', None, 'ERROR']:
                    msg_id = f'{i}_{status}'
                    for item in make_wal_items(msg_id, status=status):
                        storage.op(item)
            assert storage.current_wal_size == 6 * 100 + 7 * 2 * 100
            assert storage.should_compact()
        # test read from disk and compact
        with ActorLocalStorage(dir_path, wal_limit=550) as storage:
            assert storage.current_wal_size == 6 * 100 + 7 * 2 * 100
            assert storage.should_compact()
            storage.compact()
            assert storage.current_wal_size == 550
        # test read from disk after compact
        with ActorLocalStorage(dir_path, wal_limit=550) as storage:
            assert not storage.should_compact()
            assert storage.current_wal_size == 550


def test_actor_memory_storage():
    with ActorMemoryStorage() as storage:
        for i in range(100):
            for status in ['OK', None, 'ERROR']:
                msg_id = f'{i}_{status}'
                for item in make_wal_items(msg_id, status=status):
                    storage.op(item)
        assert len(set(storage.query_send_messages())) == 100
