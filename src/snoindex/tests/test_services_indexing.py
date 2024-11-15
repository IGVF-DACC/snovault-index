import pytest


def test_services_indexing_get_uuid_and_version_from_message(
        mock_invalidation_message,
):
    from snoindex.services.indexing import get_uuid_and_version_from_message
    actual = get_uuid_and_version_from_message(
        mock_invalidation_message
    )
    expected = ('09d05b87-4d30-4dfb-b243-3327005095f2', 1234)
    assert actual == expected


@pytest.mark.integration
def test_services_indexing_indexing_service_init(
        indexing_service_props,
):
    from snoindex.services.indexing import IndexingService
    indexing_service = IndexingService(
        props=indexing_service_props
    )
    assert isinstance(indexing_service, IndexingService)


@pytest.mark.integration
def test_services_indexing_indexing_service_handle_message(
        indexing_service,
        mock_invalidation_message,
        get_all_results,
):
    results = list(
        get_all_results(
            indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 0
    indexing_service.handle_message(
        mock_invalidation_message,
    )
    indexing_service.props.opensearch.refresh_resources_index()
    assert len(indexing_service.tracker.handled_messages) == 1
    results = list(
        get_all_results(
            indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 1


def raise_error(*args, **kwargs):
    raise Exception('something went wrong')


@pytest.mark.integration
def test_services_indexing_indexing_service_try_to_handle_messages_error(
        indexing_service,
        mock_invalidation_message,
        get_all_results,
        mocker,
):
    results = list(
        get_all_results(
            indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 0
    indexing_service.tracker.add_new_messages(
        [
            mock_invalidation_message
        ]
    )
    indexing_service.try_to_handle_messages()
    indexing_service.props.opensearch.refresh_resources_index()
    assert len(indexing_service.tracker.handled_messages) == 1
    results = list(
        get_all_results(
            indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 1
    assert indexing_service.tracker.number_failed_messages == 0
    mocker.patch(
        'snoindex.services.indexing.IndexingService.handle_message',
        raise_error,
    )
    indexing_service.try_to_handle_messages()
    assert indexing_service.tracker.number_failed_messages == 1


@pytest.mark.integration
def test_services_indexing_indexing_service_get_new_messages_from_queue(
        indexing_service,
        mock_invalidation_message_outbound,
):
    indexing_service.props.invalidation_queue.send_messages(
        [
            mock_invalidation_message_outbound,
        ]
    )
    assert int(
        indexing_service.props.invalidation_queue.info()[
            'ApproximateNumberOfMessages'
        ]
    ) == 1
    indexing_service.get_new_messages_from_queue()
    assert indexing_service.tracker.number_all_messages == 1
    assert indexing_service.tracker.number_handled_messages == 0
    assert indexing_service.tracker.number_failed_messages == 0
    indexing_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_indexing_indexing_service_try_to_handle_messages_okay(
        indexing_service,
        mock_invalidation_message_outbound,
):
    assert len(indexing_service.tracker.new_messages) == 0
    indexing_service.props.invalidation_queue.send_messages(
        [
            mock_invalidation_message_outbound,
        ]
    )
    assert int(
        indexing_service.props.invalidation_queue.info()[
            'ApproximateNumberOfMessages'
        ]
    ) == 1
    indexing_service.get_new_messages_from_queue()
    assert len(indexing_service.tracker.new_messages) == 1
    assert len(indexing_service.tracker.handled_messages) == 0
    indexing_service.try_to_handle_messages()
    assert len(indexing_service.tracker.handled_messages) == 1
    assert indexing_service.tracker.number_handled_messages == 1
    assert indexing_service.tracker.number_failed_messages == 0
    indexing_service.mark_handled_messages_as_processed()
    indexing_service.props.invalidation_queue.wait_for_queue_to_drain(
        number_of_checks=1,
        seconds_between_checks=1,
    )
    assert int(
        indexing_service.props.invalidation_queue.info()[
            'ApproximateNumberOfMessages'
        ]
    ) == 0
    indexing_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_indexing_indexing_service_run_once(
        indexing_service,
        mock_invalidation_message_outbound,
        get_all_results,
):
    indexing_service.props.invalidation_queue.send_messages(
        [
            mock_invalidation_message_outbound,
        ]
    )
    indexing_service.run_once()
    assert len(indexing_service.tracker.new_messages) == 0
    assert len(indexing_service.tracker.failed_messages) == 0
    assert len(indexing_service.tracker.handled_messages) == 0
    assert indexing_service.tracker.stats()['all'] == 1
    assert indexing_service.tracker.stats()['handled'] == 1
    assert indexing_service.tracker.stats()['failed'] == 0
    indexing_service.props.opensearch.refresh_resources_index()
    results = list(
        get_all_results(
            indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 1
    indexing_service.props.invalidation_queue.wait_for_queue_to_drain(
        number_of_checks=1,
        seconds_between_checks=1,
    )
    assert int(
        indexing_service.props.invalidation_queue.info()[
            'ApproximateNumberOfMessages'
        ]
    ) == 0
    indexing_service.props.invalidation_queue.clear()


@pytest.mark.integration
def test_services_indexing_bulk_indexing_service_init(
        bulk_indexing_service_props,
):
    from snoindex.services.indexing import BulkIndexingService
    bulk_indexing_service = BulkIndexingService(
        props=bulk_indexing_service_props
    )
    assert isinstance(bulk_indexing_service, BulkIndexingService)


@pytest.mark.integration
def test_services_indexing_bulk_indexing_service_try_to_handle_messages_error(
        bulk_indexing_service,
        mock_invalidation_message,
        get_all_results,
        mocker,
):
    results = list(
        get_all_results(
            bulk_indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 0
    bulk_indexing_service.tracker.add_new_messages(
        [
            mock_invalidation_message
        ]
    )
    bulk_indexing_service.try_to_handle_messages()
    bulk_indexing_service.props.opensearch.refresh_resources_index()
    assert len(bulk_indexing_service.tracker.handled_messages) == 1
    results = list(
        get_all_results(
            bulk_indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 1
    assert bulk_indexing_service.tracker.number_failed_messages == 0
    mocker.patch(
        'snoindex.services.indexing.BulkIndexingService.handle_messages',
        raise_error,
    )
    bulk_indexing_service.try_to_handle_messages()
    assert bulk_indexing_service.tracker.number_failed_messages == 1


@pytest.mark.integration
def test_services_indexing_bulk_indexing_service_run_once(
        bulk_indexing_service,
        mock_invalidation_message_outbound,
        get_all_results,
):
    bulk_indexing_service.props.bulk_invalidation_queue.send_messages(
        [
            mock_invalidation_message_outbound,
        ]
    )
    bulk_indexing_service.run_once()
    assert len(bulk_indexing_service.tracker.new_messages) == 0
    assert len(bulk_indexing_service.tracker.failed_messages) == 0
    assert len(bulk_indexing_service.tracker.handled_messages) == 0
    assert bulk_indexing_service.tracker.stats()['all'] == 1
    assert bulk_indexing_service.tracker.stats()['handled'] == 1
    assert bulk_indexing_service.tracker.stats()['failed'] == 0
    bulk_indexing_service.props.opensearch.refresh_resources_index()
    results = list(
        get_all_results(
            bulk_indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 1
    bulk_indexing_service.props.bulk_invalidation_queue.wait_for_queue_to_drain(
        number_of_checks=1,
        seconds_between_checks=1,
    )
    assert int(
        bulk_indexing_service.props.bulk_invalidation_queue.info()[
            'ApproximateNumberOfMessages'
        ]
    ) == 0
    bulk_indexing_service.props.bulk_invalidation_queue.clear()


@pytest.mark.integration
def test_services_run_invalidation_and_indexing_services_together(
        invalidation_service,
        indexing_service,
        get_all_results,
        mock_transaction_message_outbound,
        mocked_portal,
):
    item = mocked_portal.get_item('4cead359-10e9-49a8-9d20-f05b2499b919')
    invalidation_service.props.opensearch.index_item(item)
    invalidation_service.props.transaction_queue.send_messages(
        [
            mock_transaction_message_outbound
        ]
    )
    invalidation_service.run_once()
    indexing_service.run_once()
    results = list(
        get_all_results(
            indexing_service.props.opensearch.props.client
        )['hits']['hits']
    )
    assert len(results) == 1
    invalidation_service.props.transaction_queue.clear()
    indexing_service.props.invalidation_queue.clear()
