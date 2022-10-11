import pytest


def test_config_get_sqs_client():
    from botocore.client import BaseClient
    from snoindex.config import get_sqs_client
    client = get_sqs_client('http://localstackendpoint:4566')
    assert isinstance(client, BaseClient)


def test_config_get_opensearch_client():
    from opensearchpy import OpenSearch
    from snoindex.config import get_opensearch_client
    client = get_opensearch_client('http://opensearch')
    assert isinstance(client, OpenSearch)


@pytest.mark.integration
def test_config_invalidation_service_config():
    from snoindex.config import InvalidationServiceConfig
    assert False


@pytest.mark.integration
def test_config_indexing_service_config(invalidation_queue, opensearch_client):
    from snoindex.config import IndexingServiceConfig
    from snoindex.config import get_sqs_client
    config = IndexingServiceConfig(
        backend_url='some-url',
        auth=('some', 'auth'),
        invalidation_queue=invalidation_queue,
        opensearch_client=opensearch_client,
        opensearch_resources_index='some-index',
        sqs_client=get_sqs_client('http://localstackendpoint:4566')
    )
    assert config.backend_url == 'some-url'
    assert isinstance(config, IndexingServiceConfig)
