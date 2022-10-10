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
def test_services_indexing_indexing_service_try_to_handle_messages(
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
def test_services_indexing_indexing_service_mark_handled_messages_as_processed():
    assert False


@pytest.mark.integration
def test_services_indexing_indexing_service_get_new_messages_from_queue():
    assert False


@pytest.mark.integration
def test_services_indexing_indexing_service_clear():
    assert False


@pytest.mark.integration
def test_services_indexing_indexing_service_run_once():
    assert False
