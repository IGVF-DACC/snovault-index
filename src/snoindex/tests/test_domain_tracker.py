import pytest


def test_domain_tracker_message_tracker_init():
    from snoindex.domain.tracker import MessageTracker
    mt = MessageTracker()
    assert isinstance(mt, MessageTracker)
    assert mt.number_all_messages == 0
    assert mt.number_handled_messages == 0
    assert mt.number_failed_messages == 0
    assert mt.new_messages == []
    assert mt.handled_messages == []
    assert mt.failed_messages == []


def test_domain_tracker_message_tracker_clear_stats(inbound_message):
    from snoindex.domain.tracker import MessageTracker
    mt = MessageTracker()
    mt.add_new_messages([inbound_message])
    assert mt.number_all_messages == 1
    mt.clear_stats()
    assert mt.number_all_messages == 0
    assert len(mt.new_messages) == 1


def test_domain_tracker_message_tracker_clear(inbound_message):
    from snoindex.domain.tracker import MessageTracker
    mt = MessageTracker()
    messages = [inbound_message for i in range(3)]
    mt.add_new_messages(messages)
    mt.add_failed_messages(messages)
    mt.add_handled_messages(messages)
    assert mt.number_all_messages == 3
    assert mt.number_handled_messages == 3
    assert mt.number_failed_messages == 3
    assert mt.new_messages == messages
    assert mt.handled_messages == messages
    assert mt.failed_messages == messages
    mt.clear()
    assert mt.number_all_messages == 3
    assert mt.number_handled_messages == 3
    assert mt.number_failed_messages == 3
    assert mt.new_messages == []
    assert mt.handled_messages == []
    assert mt.failed_messages == []


def test_domain_tracker_message_tracker_add_new_messages(inbound_message):
    from snoindex.domain.tracker import MessageTracker
    mt = MessageTracker()
    messages = [inbound_message for i in range(3)]
    mt.add_new_messages(messages)
    mt.number_all_messages == 3
    mt.new_messages == messages
    mt.clear()
    mt.add_new_messages(messages)
    mt.add_new_messages(messages)
    mt.number_all_messages == 9
    assert len(mt.new_messages) == 6
    mt.clear()
    assert mt.new_messages == []
    mt.number_all_messages == 9


def test_domain_tracker_message_tracker_add_handled_messages(inbound_message):
    from snoindex.domain.tracker import MessageTracker
    mt = MessageTracker()
    messages = [inbound_message for i in range(3)]
    mt.add_handled_messages(messages)
    mt.number_handled_messages == 3
    mt.handled_messages == messages
    mt.clear()
    mt.add_handled_messages(messages)
    mt.add_handled_messages(messages)
    mt.number_handled_messages == 9
    assert len(mt.handled_messages) == 6
    mt.clear()
    assert mt.handled_messages == []
    mt.number_handled_messages == 9


def test_domain_tracker_message_tracker_add_failed_messages(inbound_message):
    from snoindex.domain.tracker import MessageTracker
    mt = MessageTracker()
    messages = [inbound_message for i in range(3)]
    mt.add_failed_messages(messages)
    mt.number_failed_messages == 3
    mt.failed_messages == messages
    mt.clear()
    mt.add_failed_messages(messages)
    mt.add_failed_messages(messages)
    mt.number_failed_messages == 9
    assert len(mt.failed_messages) == 6
    mt.clear()
    assert mt.failed_messages == []
    mt.number_failed_messages == 9


def test_domain_tracker_message_tracker_stats(inbound_message):
    from snoindex.domain.tracker import MessageTracker
    mt = MessageTracker()
    messages = [inbound_message for i in range(3)]
    mt.add_new_messages(messages)
    mt.add_failed_messages(messages)
    mt.add_handled_messages(messages)
    mt.add_new_messages(messages[:1])
    mt.add_failed_messages(messages[:3])
    mt.add_handled_messages(messages[:2])
    mt.add_handled_messages([messages[0]])
    assert mt.stats() == {
        'all': 4,
        'handled': 6,
        'failed': 6
    }
    mt.add_handled_messages([messages[0]])
    assert mt.stats() == {
        'all': 4,
        'handled': 7,
        'failed': 6
    }
    mt.add_new_messages(messages)
    assert mt.stats() == {
        'all': 7,
        'handled': 7,
        'failed': 6
    }
