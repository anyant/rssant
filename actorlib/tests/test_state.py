from actorlib.state import ActorState, OK, ERROR
from actorlib.message import ActorMessage


MSG_PARENT = ActorMessage(
    id='parent',
    content=dict(value='parent'),
    priority=1,
    src='test',
    src_node='test_node',
    dst='test',
    dst_node='test_node',
    require_ack=True,
)

MSG_TEST = ActorMessage(
    id='test',
    content=dict(value='test'),
    priority=1,
    src='test',
    src_node='test_node',
    dst='test',
    dst_node='test_node',
    require_ack=True,
    parent_id=MSG_PARENT.id,
)

MSG_NOACK = ActorMessage(
    id='noack',
    content=dict(value='noack'),
    priority=1,
    src='test',
    src_node='test_node',
    dst='test',
    dst_node='test_node',
    require_ack=False,
    parent_id=MSG_PARENT.id,
)


def assert_message_not_exists(state, msg):
    assert state.message_objects.get(msg.id) is None


def test_complete():
    s = ActorState()
    s.apply_complete(message_id=MSG_TEST.id, status=OK)
    assert s.complete_message_state[MSG_TEST.id] == OK
    assert_message_not_exists(s, MSG_TEST)


def test_inbox_execute_complete():
    s = ActorState()
    s.apply_inbox(message=MSG_TEST)
    s.apply_execute(message_id=MSG_TEST.id)
    s.apply_done(message_id=MSG_TEST.id, status=OK)
    s.apply_complete(message_id=MSG_TEST.id)
    assert s.complete_message_state[MSG_TEST.id] == OK
    assert not s.state
    assert_message_not_exists(s, MSG_TEST)


def test_inbox_execute_outbox_export_ack_complete():
    s = ActorState()
    s.apply_inbox(message=MSG_PARENT)
    s.apply_execute(message_id=MSG_PARENT.id)
    s.apply_outbox(message_id=MSG_PARENT.id, outbox_messages=[MSG_TEST])
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_acked(outbox_message_id=MSG_TEST.id, status=OK)
    s.apply_complete(message_id=MSG_PARENT.id)
    assert s.complete_message_state[MSG_PARENT.id] == OK
    assert_message_not_exists(s, MSG_PARENT)
    assert_message_not_exists(s, MSG_TEST)


def test_inbox_execute_outbox_export_no_ack_complete():
    s = ActorState()
    s.apply_inbox(message=MSG_PARENT)
    s.apply_execute(message_id=MSG_PARENT.id)
    s.apply_outbox(message_id=MSG_PARENT.id, outbox_messages=[MSG_NOACK])
    s.apply_export(outbox_message_id=MSG_NOACK.id)
    s.apply_complete(message_id=MSG_PARENT.id)
    assert s.complete_message_state[MSG_PARENT.id] == OK
    assert_message_not_exists(s, MSG_PARENT)
    assert_message_not_exists(s, MSG_NOACK)


def test_inbox_execute_outbox_retry_complete():
    s = ActorState()
    s.apply_inbox(message=MSG_PARENT)
    s.apply_execute(message_id=MSG_PARENT.id)
    s.apply_outbox(message_id=MSG_PARENT.id, outbox_messages=[MSG_TEST])
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_retry(outbox_message_id=MSG_TEST.id)
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_acked(outbox_message_id=MSG_TEST.id, status=ERROR)
    s.apply_retry(outbox_message_id=MSG_TEST.id)
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_acked(outbox_message_id=MSG_TEST.id, status=OK)
    s.apply_complete(message_id=MSG_PARENT.id)
    assert s.complete_message_state[MSG_PARENT.id] == OK
    assert_message_not_exists(s, MSG_PARENT)
    assert_message_not_exists(s, MSG_TEST)


def test_inbox_execute_outbox_export_ack_no_ack_retry_complete():
    s = ActorState()
    s.apply_inbox(message=MSG_PARENT)
    s.apply_execute(message_id=MSG_PARENT.id)
    s.apply_outbox(message_id=MSG_PARENT.id, outbox_messages=[MSG_TEST, MSG_NOACK])
    s.apply_export(outbox_message_id=MSG_NOACK.id)
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_retry(outbox_message_id=MSG_TEST.id)
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_acked(outbox_message_id=MSG_TEST.id, status=ERROR)
    s.apply_retry(outbox_message_id=MSG_TEST.id)
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_acked(outbox_message_id=MSG_TEST.id, status=OK)
    s.apply_complete(message_id=MSG_PARENT.id)
    assert s.complete_message_state[MSG_PARENT.id] == OK
    assert_message_not_exists(s, MSG_PARENT)
    assert_message_not_exists(s, MSG_TEST)
    assert_message_not_exists(s, MSG_NOACK)


def test_notify():
    s = ActorState()
    s.apply_notify(dst='test', src_node='test_node', available=True)
    assert 'test_node' in s.upstream[MSG_TEST.id]


def test_restart():
    s = ActorState()
    s.apply_inbox(message=MSG_TEST)
    s.apply_execute(message_id=MSG_TEST.id)
    s.apply_restart()
    assert s.state == {MSG_TEST.id: {'status': ERROR}}
    s.apply_complete(message_id=MSG_TEST.id)
    assert s.complete_message_state[MSG_TEST.id] == ERROR
    assert_message_not_exists(s, MSG_TEST)


def test_dump_simple():
    s = ActorState()
    s.apply_inbox(message=MSG_TEST)
    s.apply_execute(message_id=MSG_TEST.id)
    wal_items = s.dump()
    s = ActorState()
    for item in wal_items:
        s.apply(**item)
    s.apply_done(message_id=MSG_TEST.id, status=OK)
    s.apply_complete(message_id=MSG_TEST.id)
    assert s.complete_message_state[MSG_TEST.id] == OK
    assert_message_not_exists(s, MSG_TEST)


def test_dump_complex():
    s = ActorState()
    s.apply_inbox(message=MSG_PARENT)
    s.apply_execute(message_id=MSG_PARENT.id)
    s.apply_outbox(message_id=MSG_PARENT.id, outbox_messages=[MSG_TEST, MSG_NOACK])
    s.apply_export(outbox_message_id=MSG_NOACK.id)
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_retry(outbox_message_id=MSG_TEST.id)
    s.apply_export(outbox_message_id=MSG_TEST.id)
    s.apply_acked(outbox_message_id=MSG_TEST.id, status=ERROR)
    s.apply_retry(outbox_message_id=MSG_TEST.id)
    s.apply_export(outbox_message_id=MSG_TEST.id)
    wal_items = s.dump()
    s = ActorState()
    for item in wal_items:
        s.apply(**item)
    s.apply_acked(outbox_message_id=MSG_TEST.id, status=OK)
    s.apply_complete(message_id=MSG_PARENT.id)
    assert s.complete_message_state[MSG_PARENT.id] == OK
    assert_message_not_exists(s, MSG_PARENT)
    assert_message_not_exists(s, MSG_TEST)
    assert_message_not_exists(s, MSG_NOACK)
