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


def test_services_indexing_indexing_service_init():
    assert False


@pytest.mark.integration
def test_services_indexing_indexing_service_handle_message():
    assert False


@pytest.mark.integration
def test_services_indexing_indexing_service_try_to_handle_messages():
    assert False


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
