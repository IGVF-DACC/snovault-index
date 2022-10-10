import pytest


def test_services_invalidation_get_updated_uuids_from_transaction(mock_transaction_message):
    from snoindex.services.invalidation import get_updated_uuids_from_transaction
    assert get_updated_uuids_from_transaction(mock_transaction_message) == [
        '09d05b87-4d30-4dfb-b243-3327005095f2'
    ]


def test_services_invalidation_get_renamed_uuids_from_transaction(mock_transaction_message):
    from snoindex.services.invalidation import get_renamed_uuids_from_transaction
    assert get_renamed_uuids_from_transaction(mock_transaction_message) == [
        '09d05b87-4d30-4dfb-b243-3327005095f2'
    ]


def test_services_invalidation_get_all_uuids_from_transaction(mock_transaction_message):
    from snoindex.services.invalidation import get_all_uuids_from_transaction
    assert get_all_uuids_from_transaction(mock_transaction_message) == [
        '09d05b87-4d30-4dfb-b243-3327005095f2'
    ]


def test_services_invalidation_make_unique_id(inbound_message):
    from snoindex.services.invalidation import make_unique_id
    assert make_unique_id('abc', '123') == 'abc-123'


def test_services_invalidation_make_outbound_message(mock_transaction_message):
    from snoindex.services.invalidation import make_outbound_message
    from snoindex.domain.message import OutboundMessage
    outbound_message = make_outbound_message(
        mock_transaction_message,
        'someuuid'
    )
    assert isinstance(outbound_message, OutboundMessage)
    assert outbound_message.unique_id == 'someuuid-1234'
    assert outbound_message.body == {
        'metadata': {
            'xid': 1234,
            'tid': 'abcd'
        },
        'data': {
            'uuid': 'someuuid'
        }
    }


@pytest.mark.integration
def test_services_invalidation_invalidation_service_init(
        transaction_queue,
        invalidation_queue,
        opensearch_repository
):
    from snoindex.services.invalidation import InvalidationServiceProps
    from snoindex.services.invalidation import InvalidationService
    props = InvalidationServiceProps(
        transaction_queue=transaction_queue,
        invalidation_queue=invalidation_queue,
        opensearch=opensearch_repository,
    )
    invalidation_service = InvalidationService(
        props=props
    )
    assert isinstance(invalidation_service, InvalidationService)


@pytest.mark.integration
def test_services_invalidation_invalidation_service_invalidate_all_uuids_from_transaction(
        invalidation_service,
        mock_transaction_message
):
    invalidation_service.invalidate_all_uuids_from_transaction(
        mock_transaction_message
    )
    message = list(
        invalidation_service.props.invalidation_queue.get_messages(
            desired_number_of_messages=1
        )
    )[0]
    assert message.json_body['data']['uuid'] == '09d05b87-4d30-4dfb-b243-3327005095f2'
    invalidation_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_invalidation_invalidation_service_invalidate_all_related_uuids(
        invalidation_service,
        mock_transaction_message,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    invalidation_service.props.opensearch.index_item(item)
    invalidation_service.invalidate_all_related_uuids(mock_transaction_message)
    message = list(
        invalidation_service.props.invalidation_queue.get_messages(
            desired_number_of_messages=1
        )
    )[0]
    assert message.json_body['data']['uuid'] == '4cead359-10e9-49a8-9d20-f05b2499b919'
    invalidation_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_invalidation_invalidation_service_handle_message(
        invalidation_service,
        mock_transaction_message,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    invalidation_service.props.opensearch.index_item(item)
    invalidation_service.handle_message(mock_transaction_message)
    messages = list(
        invalidation_service.props.invalidation_queue.get_messages(
            desired_number_of_messages=2
        )
    )
    uuids = [
        message.json_body['data']['uuid']
        for message
        in messages
    ]
    assert '09d05b87-4d30-4dfb-b243-3327005095f2' in uuids
    assert '4cead359-10e9-49a8-9d20-f05b2499b919' in uuids
    assert len(invalidation_service.tracker.handled_messages) == 1
    assert invalidation_service.tracker.stats()['handled'] == 1
    invalidation_service.props.invalidation_queue.clear()


def raise_error(*args, **kwargs):
    raise Exception('something went wrong')


@pytest.mark.integration
def test_services_invalidation_invalidation_service_try_to_handle_message(
        invalidation_service,
        mock_transaction_message,
        mocked_portal,
        mocker,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    invalidation_service.props.opensearch.index_item(item)
    mocker.patch(
        'snoindex.services.invalidation.InvalidationService.handle_message',
        raise_error,
    )
    invalidation_service.tracker.add_new_messages([mock_transaction_message])
    invalidation_service.try_to_handle_messages()
    assert len(invalidation_service.tracker.handled_messages) == 0
    assert len(invalidation_service.tracker.failed_messages) == 1
    invalidation_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_invalidation_invalidation_service_mark_handled_messages_as_processed(
        invalidation_service,
        mock_transaction_message_outbound,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    invalidation_service.props.opensearch.index_item(item)
    invalidation_service.props.transaction_queue.send_messages(
        [
            mock_transaction_message_outbound
        ]
    )
    assert int(
        invalidation_service.props.transaction_queue.info()[
            'ApproximateNumberOfMessages']
    ) == 1
    invalidation_service.get_new_messages_from_queue()
    invalidation_service.try_to_handle_messages()
    messages = list(
        invalidation_service.props.invalidation_queue.get_messages(
            desired_number_of_messages=2
        )
    )
    uuids = [
        message.json_body['data']['uuid']
        for message
        in messages
    ]
    assert '09d05b87-4d30-4dfb-b243-3327005095f2' in uuids
    assert '4cead359-10e9-49a8-9d20-f05b2499b919' in uuids
    assert len(invalidation_service.tracker.new_messages) == 1
    assert len(invalidation_service.tracker.handled_messages) == 1
    assert invalidation_service.tracker.stats()['handled'] == 1
    assert invalidation_service.tracker.stats()['all'] == 1
    assert invalidation_service.tracker.stats()['failed'] == 0
    invalidation_service.mark_handled_messages_as_processed()
    assert int(
        invalidation_service.props.transaction_queue.info()[
            'ApproximateNumberOfMessages'
        ]
    ) == 0
    assert int(
        invalidation_service.props.transaction_queue.info()[
            'ApproximateNumberOfMessagesNotVisible'
        ]
    ) == 0
    invalidation_service.props.transaction_queue.clear()
    invalidation_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_invalidation_invalidation_service_get_new_messages_from_queue(
        invalidation_service,
):
    from snoindex.domain.message import OutboundMessage
    messages = []
    for i in range(10):
        message_body = {
            'metadata': {
                'xid': int(f'1234{i}'),
                'tid': f'abcd{i}',
            },
            'data': {
                'payload': {
                    'updated': [
                        '09d05b87-4d30-4dfb-b243-3327005095f2',
                    ],
                    'renamed': [
                        '09d05b87-4d30-4dfb-b243-3327005095f2',
                    ]
                }
            }
        }
        messages.append(
            OutboundMessage(
                unique_id=message_body['metadata']['tid'],
                body=message_body,
            )
        )
    invalidation_service.props.transaction_queue.send_messages(messages)
    for i in range(10):
        invalidation_service.get_new_messages_from_queue()
    assert invalidation_service.tracker.number_all_messages == 10
    invalidation_service.props.transaction_queue.mark_as_processed(
        invalidation_service.tracker.new_messages
    )
    invalidation_service.props.transaction_queue.clear()


@pytest.mark.integration
def test_services_invalidation_invalidation_service_run_once(
        invalidation_service,
        mock_transaction_message_outbound,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    invalidation_service.props.opensearch.index_item(item)
    invalidation_service.props.transaction_queue.send_messages(
        [
            mock_transaction_message_outbound
        ]
    )
    invalidation_service.run_once()
    messages = list(
        invalidation_service.props.invalidation_queue.get_messages(
            desired_number_of_messages=2
        )
    )
    uuids = [
        message.json_body['data']['uuid']
        for message
        in messages
    ]
    assert '09d05b87-4d30-4dfb-b243-3327005095f2' in uuids
    assert '4cead359-10e9-49a8-9d20-f05b2499b919' in uuids
    assert len(invalidation_service.tracker.new_messages) == 0
    assert len(invalidation_service.tracker.failed_messages) == 0
    assert len(invalidation_service.tracker.handled_messages) == 0
    assert invalidation_service.tracker.stats()['all'] == 1
    assert invalidation_service.tracker.stats()['handled'] == 1
    assert invalidation_service.tracker.stats()['failed'] == 0
    invalidation_service.props.transaction_queue.clear()
    invalidation_service.props.invalidation_queue.clear()
