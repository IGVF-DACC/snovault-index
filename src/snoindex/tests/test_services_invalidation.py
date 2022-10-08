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
        mock_transaction_message, 'someuuid')
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
        opensearch=opensearch_repository
    )
    invalidation_service = InvalidationService(
        props=props
    )
    assert isinstance(invalidation_service, InvalidationService)


def test_services_invalidation_invalidation_service_invalidate_all_uuids_from_transaction(
        invalidation_service,
        mock_transaction_message
):
    invalidation_service.invalidate_all_uuids_from_transaction(
        mock_transaction_message)
    message = list(
        invalidation_service.props.invalidation_queue.get_messages(
            desired_number_of_messages=1
        )
    )[0]
    assert message.json_body['data']['uuid'] == '09d05b87-4d30-4dfb-b243-3327005095f2'
    invalidation_service.props.invalidation_queue.clear()


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


def test_services_invalidation_invalidation_service_handle_message():
    assert False


def test_services_invalidation_invalidation_service_try_to_handle_message():
    assert False


def test_services_invalidation_invalidation_service_mark_handled_messages_as_processed():
    assert False


def test_services_invalidation_invalidation_service_get_new_messages_from_queue():
    assert False


def test_services_invalidation_invalidation_service_log_stats():
    assert False


def test_services_invalidation_invalidation_service_clear():
    assert False


def test_services_invalidation_invalidation_service_run_once():
    assert False


def test_services_invalidation_invalidation_service_poll():
    assert False
