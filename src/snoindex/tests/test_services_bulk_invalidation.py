import pytest


@pytest.mark.integration
def test_services_bulk_invalidation_bulk_invalidation_service_init(
        transaction_queue,
        invalidation_queue,
        opensearch_repository
):
    from snoindex.services.invalidation import BulkInvalidationServiceProps
    from snoindex.services.invalidation import BulkInvalidationService
    props = BulkInvalidationServiceProps(
        transaction_queue=transaction_queue,
        invalidation_queue=invalidation_queue,
        opensearch=opensearch_repository,
        messages_to_handle_per_run=100,
        related_uuids_search_batch_size=10,
    )
    bulk_invalidation_service = BulkInvalidationService(
        props=props
    )
    assert isinstance(bulk_invalidation_service, BulkInvalidationService)


@pytest.mark.integration
def test_services_bulk_invalidation_bulk_invalidation_service_parse_uuids_from_messages(
        bulk_invalidation_service,
        mock_transaction_message,
        bulk_mock_transaction_messages,
):
    all_uuids, all_updated_uuids, all_renamed_uuids = bulk_invalidation_service.parse_uuids_from_messages(
        [
            mock_transaction_message,
        ]
    )
    assert all_uuids == {'09d05b87-4d30-4dfb-b243-3327005095f2'}
    assert all_updated_uuids == {'09d05b87-4d30-4dfb-b243-3327005095f2'}
    assert all_renamed_uuids == {'09d05b87-4d30-4dfb-b243-3327005095f2'}
    all_uuids, all_updated_uuids, all_renamed_uuids = bulk_invalidation_service.parse_uuids_from_messages(
        bulk_mock_transaction_messages,
    )
    assert all_uuids == {'3-updated', '4-renamed', '2-renamed', '1-updated',
                         '2-updated', '4-updated', '1-renamed', '0-updated', '3-renamed', '0-renamed'}
    assert all_updated_uuids == {
        '0-updated', '3-updated', '1-updated', '4-updated', '2-updated'}
    assert all_renamed_uuids == {
        '4-renamed', '2-renamed', '1-renamed', '3-renamed', '0-renamed'}


@pytest.mark.integration
def test_services_bulk_invalidation_bulk_invalidation_service_get_related_uuids(
        bulk_invalidation_service,
        mock_transaction_message,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    bulk_invalidation_service.props.opensearch.index_item(item)
    all_uuids, all_updated_uuids, all_renamed_uuids = bulk_invalidation_service.parse_uuids_from_messages(
        [
            mock_transaction_message,
        ]
    )
    related_uuids = bulk_invalidation_service.get_related_uuids(
        all_uuids,
        all_updated_uuids,
        all_renamed_uuids,
    )
    assert related_uuids == {'4cead359-10e9-49a8-9d20-f05b2499b919'}
    bulk_uuids = set([
        '09d05b87-4d30-4dfb-b243-3327005095f2',
        '0abbd494-b852-433c-b360-93996f679dae',
        '44324de6-c057-451f-a8f1-1d9a6cb2762c',
        '7e763d5a-634b-4181-a812-4361183359e9',
        '90be62e4-0757-4097-b5cf-2e6a20240a6f',
        'b0b9c607-f8b4-4f02-93f4-9895b461334b',
        '09d05b87-4d30-4dfb-b243-3327005095f2',
        '0abbd494-b852-433c-b360-93996f679dae',
        '3a3ffb78-7f16-4135-87d6-5a7ad1246dcb',
        '44324de6-c057-451f-a8f1-1d9a6cb2762c',
        '7e763d5a-634b-4181-a812-4361183359e9',
        '90be62e4-0757-4097-b5cf-2e6a20240a6f',
        'b0b9c607-f8b4-4f02-93f4-9895b461334b',
        'dfc72c8c-d45c-4acd-979b-49fc93cf3c62',
    ])
    related_uuids = bulk_invalidation_service.get_related_uuids(
        bulk_uuids,
        bulk_uuids,
        bulk_uuids,
    )
    assert related_uuids == {'4cead359-10e9-49a8-9d20-f05b2499b919'}


@pytest.mark.integration
def test_services_bulk_invalidation_bulk_invalidation_service_handle_messages(
        bulk_invalidation_service,
        mock_transaction_message,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    bulk_invalidation_service.props.opensearch.index_item(item)
    bulk_invalidation_service.handle_messages([mock_transaction_message])
    messages = list(
        bulk_invalidation_service.props.invalidation_queue.get_messages(
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
    assert len(bulk_invalidation_service.tracker.handled_messages) == 1
    assert bulk_invalidation_service.tracker.stats()['handled'] == 1
    bulk_invalidation_service.props.invalidation_queue.clear()


def raise_error(*args, **kwargs):
    raise Exception('something went wrong')


@pytest.mark.integration
def test_services_bulk_invalidation_bulk_invalidation_service_try_to_handle_messages(
        bulk_invalidation_service,
        mock_transaction_message,
        mocked_portal,
        mocker,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    bulk_invalidation_service.props.opensearch.index_item(item)
    mocker.patch(
        'snoindex.services.invalidation.BulkInvalidationService.handle_messages',
        raise_error,
    )
    bulk_invalidation_service.tracker.add_new_messages(
        [mock_transaction_message])
    bulk_invalidation_service.try_to_handle_messages()
    assert len(bulk_invalidation_service.tracker.handled_messages) == 0
    assert len(bulk_invalidation_service.tracker.failed_messages) == 1
    bulk_invalidation_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_bulk_invalidation_bulk_invalidation_service_mark_handled_messages_as_processed(
        bulk_invalidation_service,
        mock_transaction_message_outbound,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    bulk_invalidation_service.props.opensearch.index_item(item)
    bulk_invalidation_service.props.transaction_queue.send_messages(
        [
            mock_transaction_message_outbound
        ]
    )
    assert int(
        bulk_invalidation_service.props.transaction_queue.info()[
            'ApproximateNumberOfMessages']
    ) == 1
    bulk_invalidation_service.get_new_messages_from_queue()
    bulk_invalidation_service.try_to_handle_messages()
    messages = list(
        bulk_invalidation_service.props.invalidation_queue.get_messages(
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
    assert len(bulk_invalidation_service.tracker.new_messages) == 1
    assert len(bulk_invalidation_service.tracker.handled_messages) == 1
    assert bulk_invalidation_service.tracker.stats()['handled'] == 1
    assert bulk_invalidation_service.tracker.stats()['all'] == 1
    assert bulk_invalidation_service.tracker.stats()['failed'] == 0
    bulk_invalidation_service.mark_handled_messages_as_processed()
    assert int(
        bulk_invalidation_service.props.transaction_queue.info()[
            'ApproximateNumberOfMessages'
        ]
    ) == 0
    assert int(
        bulk_invalidation_service.props.transaction_queue.info()[
            'ApproximateNumberOfMessagesNotVisible'
        ]
    ) == 0
    bulk_invalidation_service.props.transaction_queue.clear()
    bulk_invalidation_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_bulk_invalidation_bulk_invalidation_service_get_new_messages_from_queue(
        bulk_invalidation_service,
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
    bulk_invalidation_service.props.transaction_queue.send_messages(messages)
    bulk_invalidation_service.get_new_messages_from_queue()
    assert bulk_invalidation_service.tracker.number_all_messages == 10
    bulk_invalidation_service.props.transaction_queue.mark_as_processed(
        bulk_invalidation_service.tracker.new_messages
    )
    bulk_invalidation_service.props.transaction_queue.clear()


@pytest.mark.integration
def test_services_bulk_invalidation_bulk_invalidation_service_run_once(
        bulk_invalidation_service,
        mock_transaction_message_outbound,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919', 4)
    bulk_invalidation_service.props.opensearch.index_item(item)
    bulk_invalidation_service.props.transaction_queue.send_messages(
        [
            mock_transaction_message_outbound
        ]
    )
    bulk_invalidation_service.run_once()
    messages = list(
        bulk_invalidation_service.props.invalidation_queue.get_messages(
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
    assert len(bulk_invalidation_service.tracker.new_messages) == 0
    assert len(bulk_invalidation_service.tracker.failed_messages) == 0
    assert len(bulk_invalidation_service.tracker.handled_messages) == 0
    assert bulk_invalidation_service.tracker.stats()['all'] == 1
    assert bulk_invalidation_service.tracker.stats()['handled'] == 1
    assert bulk_invalidation_service.tracker.stats()['failed'] == 0
    bulk_invalidation_service.props.transaction_queue.clear()
    bulk_invalidation_service.props.invalidation_queue.clear()
